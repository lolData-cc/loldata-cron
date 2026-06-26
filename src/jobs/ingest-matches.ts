// src/jobs/ingest-matches.ts
//
// Step 2 of the pipeline. Walks the ingest worklist (apex players, least-
// recently-ingested first) and pages through each player's FULL current-patch
// ranked history (not just the last 20 — that's the main volume lever). Dedups
// GLOBALLY against `matches` (a shared apex game is fetched once, not 10×),
// then fetches each new match's DETAIL (no timeline → 2× throughput) and
// bulk-inserts into the rich `participants` schema, tier-stamped. Multi-pass
// until caught up.

import {
  INGEST_PLATFORMS,
  CONCURRENCY,
  MAX_MATCHES_PER_PLAYER,
  MATCH_ID_PAGE,
  PATCH_WINDOW_DAYS,
  INGEST_FETCH_BATCH,
  INGEST_PATCH_ONLY,
  RANKED_QUEUE_IDS,
  SEASON_START_EPOCH,
  SELECTED_TIERS,
  routingFor,
  routingForMatchId,
  type Platform,
  type RoutingRegion,
} from "../config";
import { getMatchIds, getMatch, getTimeline, DecryptError } from "../lib/riot";
import {
  getIngestWorklist,
  getPatchStartEpoch,
  existingMatchIds,
  loadTierMap,
  markIngested,
  bulkInsertMatches,
  bulkInsertMatchTeams,
  bulkInsertParticipants,
} from "../lib/db";
import { warmItems, legendaryPool, bootsPool } from "../lib/items";
import { mapPool } from "../lib/concurrency";
import { getLatestPatchPrefix, isOnPatch } from "../lib/patch";
import { log } from "../lib/logger";

const PLAYER_PAGE = Number(process.env.INGEST_PLAYER_PAGE ?? 200);

// Fetch each new match's TIMELINE too → item-purchase order (build paths /
// item-slot filter in the Explorer) + the @10 stats. It's a 2nd Riot call per
// match (≈ halves match throughput), so it's a toggle. Default ON; set
// INGEST_TIMELINE=0 to go back to detail-only / max throughput.
const INGEST_TIMELINE = (process.env.INGEST_TIMELINE ?? "1") !== "0";

// ── multi-pass "until caught up" ───────────────────────────────────
// Re-walk the worklist (least-recently-ingested first) pass after pass until a
// full pass brings in only a trickle of genuinely-new matches (diminishing
// returns) — then stop. No artificial per-run player cap: the whole selected
// base is covered, and the job still self-terminates (cron-safe).
const PASS_BATCH = Number(process.env.INGEST_BATCH ?? 30000); // players/pass (default ≥ all apex)
const CAUGHTUP_ABS = Number(process.env.INGEST_CAUGHTUP ?? 25); // absolute floor of "new" to call it done
const CAUGHTUP_FRAC = Number(process.env.INGEST_CAUGHTUP_FRAC ?? 0.03); // …or 3% of the best pass
const MAX_PASSES = Number(process.env.INGEST_MAX_PASSES ?? 4); // safety cap (full-coverage passes)

function chunkIds<T>(arr: T[], n: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

export async function ingestMatches(
  platforms: Platform[] = INGEST_PLATFORMS
): Promise<void> {
  await warmItems(); // legendary/boots classification for the per-participant build order
  const patchPrefix = await getLatestPatchPrefix();
  let startTime: number;
  if (INGEST_PATCH_ONLY) {
    // Patch-only mode (nightly): bound id pagination TIGHTLY to the current
    // patch — use the real patch-start (earliest stored game) so we don't fetch
    // old-patch games just to discard them. Fall back to a recency window.
    const patchStart = await getPatchStartEpoch(patchPrefix);
    const windowStart = Math.floor(Date.now() / 1000) - PATCH_WINDOW_DAYS * 86400;
    startTime =
      patchStart != null
        ? Math.max(patchStart - 6 * 3600, SEASON_START_EPOCH || 0)
        : Math.max(windowStart, SEASON_START_EPOCH || 0);
    log.info(
      `[ingest] PATCH-ONLY ${patchPrefix}.* (ids since ${new Date(startTime * 1000).toISOString().slice(0, 10)})`
    );
  } else {
    // Full-season mode (default): ingest EVERY ranked game regardless of patch,
    // back to the season start (0 = whole available history, capped per player
    // by MAX_MATCHES_PER_PLAYER). This is the volume mode.
    startTime = SEASON_START_EPOCH || 0;
    log.info(
      `[ingest] FULL-SEASON (all patches, ids since ${startTime ? new Date(startTime * 1000).toISOString().slice(0, 10) : "beginning of available history"}; up to ${MAX_MATCHES_PER_PLAYER}/player)`
    );
  }
  log.info(`[ingest] timeline ${INGEST_TIMELINE ? "ON (build-order + @10 stats; ~2× Riot calls/match)" : "OFF (detail only, max throughput)"}`);

  // Riot's rate limit is enforced PER routing region (europe/americas/asia/sea),
  // so platforms in DIFFERENT regions have INDEPENDENT 47/s budgets → run the
  // regions FULLY IN PARALLEL for an ~N-region throughput multiplier (EUW1+NA1+
  // KR+OC1 ≈ 4×). Platforms SHARING a region stay serial within that region —
  // parallelizing them would only multiply memory, not rate (same bucket).
  const byRegion = new Map<RoutingRegion, Platform[]>();
  for (const p of platforms) {
    const list = byRegion.get(routingFor(p));
    if (list) list.push(p);
    else byRegion.set(routingFor(p), [p]);
  }
  log.info(`[ingest] ${platforms.length} platform(s) across ${byRegion.size} routing region(s) — regions run IN PARALLEL`);
  await Promise.all(
    [...byRegion.values()].map(async (regionPlatforms) => {
      for (const platform of regionPlatforms) {
        await ingestPlatform(platform, patchPrefix, startTime);
      }
    })
  );
}

async function ingestPlatform(
  platform: Platform,
  patchPrefix: string,
  startTime: number
): Promise<void> {
  const region = routingFor(platform);
  const tierMap = await loadTierMap(platform);
  log.info(`[ingest] ${platform}: tier map ${tierMap.size} players`);

  const seenThisRun = new Set<string>(); // match-id dedup across ALL passes
  let grandTotal = 0;
  let bestPass = 0;
  let pass = 0;

  while (pass < MAX_PASSES) {
    pass++;
    const worklist = await getIngestWorklist(platform, SELECTED_TIERS, PASS_BATCH);
    if (worklist.length === 0) {
      log.info(`[ingest] ${platform}: worklist empty — nothing to do`);
      break;
    }
    // Pass 1 pages each player's FULL patch history; later passes only need the
    // most-recent games (history is immutable), so they stay fast.
    const deep = pass === 1;
    log.info(
      `[ingest] ${platform}: pass ${pass} — ${worklist.length} players (${deep ? "deep history" : "recent only"})`
    );

    let passNew = 0;
    for (let i = 0; i < worklist.length; i += PLAYER_PAGE) {
      const page = worklist.slice(i, i + PLAYER_PAGE);
      passNew += await ingestPage(page, platform, region, patchPrefix, tierMap, seenThisRun, startTime, deep, pass);
      await markIngested(page.map((p) => p.puuid));
    }

    grandTotal += passNew;
    bestPass = Math.max(bestPass, passNew);
    const threshold = Math.max(CAUGHTUP_ABS, Math.floor(bestPass * CAUGHTUP_FRAC));
    log.info(
      `[ingest] ${platform}: pass ${pass} done — ${passNew} new (run total ${grandTotal}; caught-up when ≤ ${threshold})`
    );

    // Never stop after the first pass; stop once a pass yields only a trickle.
    if (pass >= 2 && passNew <= threshold) {
      log.info(`[ingest] ${platform}: caught up after ${pass} passes`);
      break;
    }
  }

  log.info(`[ingest] ${platform}: done — ${grandTotal} new matches over ${pass} pass(es)`);
}

/** Page through one player's ranked match ids. `deep` → full patch history
 *  (paginate up to MAX_MATCHES_PER_PLAYER); else just the most-recent page. */
async function playerMatchIds(
  region: RoutingRegion,
  puuid: string,
  startTime: number,
  deep: boolean
): Promise<string[]> {
  const cap = deep ? MAX_MATCHES_PER_PLAYER : MATCH_ID_PAGE;
  const out: string[] = [];
  for (let start = 0; start < cap; start += MATCH_ID_PAGE) {
    const count = Math.min(MATCH_ID_PAGE, cap - start);
    const ids = await getMatchIds(region, puuid, {
      type: "ranked",
      start,
      count,
      startTime: startTime || undefined,
    });
    out.push(...ids);
    if (ids.length < count) break; // reached the end of their history
  }
  return out;
}

/** One page of players: collect their ranked ids (deep on pass 1), dedup
 *  (run + DB), then fetch DETAIL (no timeline) + build + BULK-insert the
 *  genuinely-new latest-patch ranked games. Returns the count ingested. */
async function ingestPage(
  page: { puuid: string }[],
  platform: Platform,
  region: RoutingRegion,
  patchPrefix: string,
  tierMap: Map<string, string>,
  seenThisRun: Set<string>,
  startTime: number,
  deep: boolean,
  pass: number
): Promise<number> {
  // 1. collect ranked match ids for the page
  const idLists = await mapPool(page, CONCURRENCY, async (p) => {
    try {
      return await playerMatchIds(region, p.puuid, startTime, deep);
    } catch (e) {
      if (e instanceof DecryptError) return [];
      log.warn(`[ingest] ids failed ${p.puuid.slice(0, 8)}:`, (e as Error)?.message);
      return [];
    }
  });

  // 2. dedup within page + against this run
  const candidate: string[] = [];
  for (const list of idLists) {
    for (const id of list) {
      if (seenThisRun.has(id)) continue;
      seenThisRun.add(id);
      candidate.push(id);
    }
  }

  // 3. global dedup against DB
  const existing = await existingMatchIds(candidate);
  const newIds = candidate.filter((id) => !existing.has(id));
  if (newIds.length === 0) return 0;

  // 4. fetch detail (+ timeline when enabled) + build, then bulk-insert — with
  // the writes PIPELINED: each batch's DB flush runs CONCURRENTLY with the next
  // batch's Riot fetches, so the rate limiter never sits idle waiting on
  // Postgres (the old code stalled all fetching during every flush ≈ 15-20% of
  // wall time). At most one flush is in flight (we await the previous before
  // starting the next), which bounds memory to ~2 batches.
  let ok = 0;
  let pendingFlush: Promise<void> = Promise.resolve();
  const flush = (
    cores: Record<string, unknown>[],
    teams: Record<string, unknown>[],
    parts: Record<string, unknown>[]
  ) => (async () => {
    await bulkInsertMatches(cores);
    await bulkInsertMatchTeams(teams);
    await bulkInsertParticipants(parts);
  })().catch((e) => log.warn(`[ingest] flush failed:`, (e as Error)?.message));

  for (const idBatch of chunkIds(newIds, INGEST_FETCH_BATCH)) {
    const cores: Record<string, unknown>[] = [];
    const teams: Record<string, unknown>[] = [];
    const parts: Record<string, unknown>[] = [];
    await mapPool(idBatch, CONCURRENCY, async (id) => {
      const matchRegion = routingForMatchId(id);
      const match = await getMatch(matchRegion, id);
      if (!match?.info) return;
      if (!RANKED_QUEUE_IDS.has(match.info.queueId)) return; // ranked solo/flex only
      if (INGEST_PATCH_ONLY && !isOnPatch(match.info.gameVersion, patchPrefix)) return;
      // 2nd Riot call (optional): the timeline gives @10 stats + item-purchase order.
      let timeline: any = null;
      if (INGEST_TIMELINE) {
        try { timeline = await getTimeline(matchRegion, id); }
        catch (e) { log.warn(`[ingest] timeline failed ${id}:`, (e as Error)?.message); }
      }
      try {
        const r = buildRows(match, timeline, tierMap); // timeline null → @10 columns null
        cores.push(r.core);
        teams.push(...r.teams);
        parts.push(...r.participants);
      } catch (e) {
        log.warn(`[ingest] build failed ${id}:`, (e as Error)?.message);
      }
    });
    // Let the PREVIOUS flush finish (it usually already did, overlapped with
    // this batch's fetch), then start this one WITHOUT blocking the next fetch.
    await pendingFlush;
    ok += cores.length;
    pendingFlush = flush(cores, teams, parts);
  }
  await pendingFlush; // drain the final batch before returning the count

  log.info(`[ingest] ${platform}: pass ${pass} — ${candidate.length} cand, ${newIds.length} new, ${ok} ingested`);
  return ok;
}

// ── parsing ────────────────────────────────────────────────────────
function platformFromMatchId(matchId: string): string {
  return matchId.split("_", 1)[0]?.toLowerCase() ?? "";
}
function toIso(ms: number): string {
  return new Date(ms).toISOString();
}
function durationSeconds(info: any): number {
  const s = info.gameStartTimestamp;
  const e = info.gameEndTimestamp;
  if (s && e && e > s) return Math.floor((e - s) / 1000);
  return Math.floor(info.gameDuration ?? 0);
}

/** @10 stats per participantId from the timeline (frame nearest 10:00,
 *  within 2 min), plus kills/deaths/assists tallied before 10:00. */
function extractAt10(timeline: any): Map<number, {
  gold: number; cs: number; xp: number; damage: number;
  kills: number; deaths: number; assists: number;
}> {
  const out = new Map<number, any>();
  const frames: any[] = timeline?.info?.frames ?? [];
  if (frames.length === 0) return out;

  // nearest frame to 600000ms, only if within 120s
  let best: any = null;
  let bestDelta = Infinity;
  for (const fr of frames) {
    const d = Math.abs((fr.timestamp ?? 0) - 600_000);
    if (d < bestDelta) { bestDelta = d; best = fr; }
  }
  const haveFrame = best && bestDelta <= 120_000;

  for (let pid = 1; pid <= 10; pid++) {
    out.set(pid, { gold: 0, cs: 0, xp: 0, damage: 0, kills: 0, deaths: 0, assists: 0 });
  }
  if (haveFrame) {
    for (const [pidStr, pf] of Object.entries<any>(best.participantFrames ?? {})) {
      const pid = Number(pidStr);
      const row = out.get(pid);
      if (!row) continue;
      row.gold = pf.totalGold ?? 0;
      row.cs = (pf.minionsKilled ?? 0) + (pf.jungleMinionsKilled ?? 0);
      row.xp = pf.xp ?? 0;
      row.damage = pf.damageStats?.totalDamageDoneToChampions ?? 0;
    }
  }
  // kills/deaths/assists before 10:00
  for (const fr of frames) {
    for (const ev of fr.events ?? []) {
      if (ev.type !== "CHAMPION_KILL" || (ev.timestamp ?? 0) > 600_000) continue;
      const killer = out.get(ev.killerId);
      if (killer) killer.kills++;
      const victim = out.get(ev.victimId);
      if (victim) victim.deaths++;
      for (const a of ev.assistingParticipantIds ?? []) {
        const ass = out.get(a);
        if (ass) ass.assists++;
      }
    }
  }
  return out;
}

/** Per-participant ordered LEGENDARY build (the slots we keep) from the timeline:
 *  ITEM_PURCHASED events restricted to the legendary pool (legendaries + Dark
 *  Seal/Mejai), ordered by time, each legendary kept once at its first completion.
 *  Replaces the giant participant_item_events table with a small int[] per row. */
function extractLegendaryOrder(timeline: any): Map<number, number[]> {
  const legendary = legendaryPool();
  const byPid = new Map<number, { ts: number; item: number }[]>();
  for (const fr of timeline?.info?.frames ?? []) {
    for (const ev of fr.events ?? []) {
      if (ev.type !== "ITEM_PURCHASED") continue;
      const pid = ev.participantId, ts = ev.timestamp, itemId = ev.itemId;
      if (!pid || ts == null || !itemId || !legendary.has(itemId)) continue;
      const arr = byPid.get(pid) ?? [];
      arr.push({ ts, item: itemId });
      byPid.set(pid, arr);
    }
  }
  const out = new Map<number, number[]>();
  for (const [pid, arr] of byPid) {
    arr.sort((a, b) => a.ts - b.ts);
    const seen = new Set<number>();
    const seq: number[] = [];
    for (const x of arr) if (!seen.has(x.item)) { seen.add(x.item); seq.push(x.item); }
    out.set(pid, seq);
  }
  return out;
}

function buildRows(match: any, timeline: any, tierMap: Map<string, string>) {
  const info = match.info;
  const matchId: string = match.metadata.matchId;

  const core = {
    match_id: matchId,
    platform: platformFromMatchId(matchId),
    game_creation: toIso(info.gameCreation),
    game_duration_seconds: durationSeconds(info),
    game_version: info.gameVersion ?? null,
    queue_id: info.queueId ?? null,
  };

  const teams = (info.teams ?? []).map((t: any) => ({
    match_id: matchId,
    team_id: t.teamId,
    win: !!t.win,
    first_dragon: t.objectives?.dragon?.first ?? null,
    first_baron: t.objectives?.baron?.first ?? null,
    towers_destroyed: t.objectives?.tower?.kills ?? null,
    dragons: t.objectives?.dragon?.kills ?? null,
    barons: t.objectives?.baron?.kills ?? null,
  }));

  // team totals for KP / damage share
  const teamKills = new Map<number, number>();
  const teamDamage = new Map<number, number>();
  for (const p of info.participants) {
    teamKills.set(p.teamId, (teamKills.get(p.teamId) ?? 0) + (p.kills ?? 0));
    teamDamage.set(
      p.teamId,
      (teamDamage.get(p.teamId) ?? 0) + (p.totalDamageDealtToChampions ?? 0)
    );
  }

  const at10 = extractAt10(timeline);
  const legByPid = extractLegendaryOrder(timeline);
  const bootsSet = bootsPool();

  const participants = info.participants.map((p: any) => {
    const tk = teamKills.get(p.teamId) ?? 0;
    const td = teamDamage.get(p.teamId) ?? 0;
    const e = at10.get(p.participantId);
    const finalItems = [p.item0, p.item1, p.item2, p.item3, p.item4, p.item5, p.item6];
    const styles = p.perks?.styles ?? [];
    const perkPrimary = (styles[0]?.selections ?? []).map((s: any) => s?.perk).filter((x: any) => x != null);
    const perkSecondary = (styles[1]?.selections ?? []).map((s: any) => s?.perk).filter((x: any) => x != null);
    const sp = p.perks?.statPerks ?? {};
    const statPerks = [sp.offense, sp.flex, sp.defense].filter((x: any) => x != null);
    return {
      match_id: matchId,
      participant_id: p.participantId,
      puuid: p.puuid ?? null,
      summoner_name: p.summonerName ?? null,
      riot_id_game_name: p.riotIdGameName ?? null,
      riot_id_tagline: p.riotIdTagline ?? null,
      team_id: p.teamId,
      champion_id: p.championId,
      champion_name: p.championName,
      role: p.teamPosition || null,
      lane: p.lane ?? null,
      win: !!p.win,
      kills: p.kills ?? 0,
      deaths: p.deaths ?? 0,
      assists: p.assists ?? 0,
      gold_earned: p.goldEarned ?? 0,
      total_damage_to_champions: p.totalDamageDealtToChampions ?? 0,
      vision_score: p.visionScore ?? 0,
      total_minions_killed: p.totalMinionsKilled ?? 0,
      neutral_minions_killed: p.neutralMinionsKilled ?? 0,
      total_cs: (p.totalMinionsKilled ?? 0) + (p.neutralMinionsKilled ?? 0),
      champ_level: p.champLevel ?? null,
      time_played: p.timePlayed ?? null,
      solo_kills: p.challenges?.soloKills ?? null,
      summoner1_id: p.summoner1Id ?? null,
      summoner2_id: p.summoner2Id ?? null,
      item0: p.item0, item1: p.item1, item2: p.item2, item3: p.item3,
      item4: p.item4, item5: p.item5, item6: p.item6,
      perk_keystone: p.perks?.styles?.[0]?.selections?.[0]?.perk ?? null,
      perk_primary_style: p.perks?.styles?.[0]?.style ?? null,
      perk_sub_style: p.perks?.styles?.[1]?.style ?? null,
      perk_primary: perkPrimary.length ? perkPrimary : null,
      perk_secondary: perkSecondary.length ? perkSecondary : null,
      stat_perks: statPerks.length ? statPerks : null,
      legendary_order: legByPid.get(p.participantId) ?? null,
      boots: finalItems.find((i) => bootsSet.has(i)) ?? null,
      kill_participation: tk > 0 ? ((p.kills ?? 0) + (p.assists ?? 0)) / tk : null,
      damage_share: td > 0 ? (p.totalDamageDealtToChampions ?? 0) / td : null,
      gold_at_10: e?.gold ?? null,
      cs_at_10: e?.cs ?? null,
      xp_at_10: e?.xp ?? null,
      damage_at_10: e?.damage ?? null,
      kills_at_10: e?.kills ?? null,
      deaths_at_10: e?.deaths ?? null,
      assists_at_10: e?.assists ?? null,
      tier: p.puuid ? tierMap.get(p.puuid) ?? null : null,
    };
  });

  return { core, teams, participants };
}
