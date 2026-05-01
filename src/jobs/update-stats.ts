/**
 * Incremental season stats updater.
 * Ingests ALL ranked matches for every Master+ EUW player,
 * stores structured data + early game stats from timeline,
 * and updates season aggregates atomically.
 */

import { SEASON_START_EPOCH, SEASON_END_EPOCH, REGIONS, CONCURRENCY } from "../config";
import type { Region } from "../config";
import { getMatchIds, getMatchDetails, getMatchTimeline, getCachedRegion, isBadPuuid } from "../riot";
import { supabase, getMasterPlusUserPuuids, upsertUsers } from "../db";
import { log } from "../logger";
import { REGION_ROUTING } from "../config";

const Q_SOLO = 420;
const Q_FLEX = 440;
const MAX_NEW_MATCHES = 1000; // Full season coverage per player

// ── Match ingestion ─────────────────────────────────────────────────

function derivePlatform(matchId: string): string {
  return matchId.split("_")[0]?.toLowerCase() ?? "";
}

function robustDuration(info: any): number {
  if (info.gameDuration > 100_000) return Math.round(info.gameDuration / 1000);
  return info.gameDuration ?? 0;
}

// In-memory set to avoid re-ingesting the same match within a single run
const _ingestedThisRun = new Set<string>();

// ── Platform prefix → Region mapping for match participant fan-out ──
const PLATFORM_TO_REGION: Record<string, Region> = {};
for (const r of REGIONS) {
  const host = REGION_ROUTING[r].platform;
  const prefix = host.split(".")[0]!;
  PLATFORM_TO_REGION[prefix] = r;
}

function platformToRegion(platform: string): Region | null {
  return PLATFORM_TO_REGION[platform.toLowerCase()] ?? null;
}

// ── Early game stats extraction from timeline ───────────────────────

interface EarlyGameStats {
  gold_at_10: number;
  cs_at_10: number;
  xp_at_10: number;
  damage_at_10: number;
  kills_at_10: number;
  deaths_at_10: number;
  assists_at_10: number;
}

function extractEarlyGameStats(
  timeline: any,
  participantCount: number,
): Map<number, EarlyGameStats> {
  const result = new Map<number, EarlyGameStats>();
  const frames = timeline?.info?.frames;
  if (!frames || frames.length === 0) return result;

  // Find the frame closest to 10 minutes (600000ms)
  let bestFrame: any = null;
  let bestDiff = Infinity;
  for (const frame of frames) {
    const diff = Math.abs((frame.timestamp ?? 0) - 600_000);
    if (diff < bestDiff) {
      bestDiff = diff;
      bestFrame = frame;
    }
  }

  // If game was shorter than ~8 minutes, skip early game stats
  if (!bestFrame || bestDiff > 120_000) return result;

  // Extract gold/cs/xp from participantFrames
  const pFrames = bestFrame.participantFrames;
  if (pFrames) {
    for (let pid = 1; pid <= participantCount; pid++) {
      const pf = pFrames[String(pid)];
      if (!pf) continue;
      result.set(pid, {
        gold_at_10: pf.totalGold ?? 0,
        cs_at_10: (pf.minionsKilled ?? 0) + (pf.jungleMinionsKilled ?? 0),
        xp_at_10: pf.xp ?? 0,
        damage_at_10: pf.damageStats?.totalDamageDoneToChampions ?? 0,
        kills_at_10: 0,
        deaths_at_10: 0,
        assists_at_10: 0,
      });
    }
  }

  // Count kills/deaths/assists from CHAMPION_KILL events before 10 min
  for (const frame of frames) {
    if ((frame.timestamp ?? 0) > 600_000) break;
    for (const ev of frame.events ?? []) {
      if (ev.type !== "CHAMPION_KILL" || (ev.timestamp ?? 0) > 600_000) continue;

      const killerId: number = ev.killerId ?? 0;
      const victimId: number = ev.victimId ?? 0;
      const assistIds: number[] = ev.assistingParticipantIds ?? [];

      if (killerId > 0) {
        const s = result.get(killerId);
        if (s) s.kills_at_10++;
      }
      if (victimId > 0) {
        const s = result.get(victimId);
        if (s) s.deaths_at_10++;
      }
      for (const aid of assistIds) {
        const s = result.get(aid);
        if (s) s.assists_at_10++;
      }
    }
  }

  return result;
}

// ── Main match ingestion function ───────────────────────────────────

async function ingestMatch(matchJson: any, region: Region): Promise<void> {
  const meta = matchJson.metadata;
  const info = matchJson.info;
  const matchId: string = meta.matchId;

  if (_ingestedThisRun.has(matchId)) return;
  _ingestedThisRun.add(matchId);

  // ── Prepare all rows (CPU-only, no awaits) ──

  const matchRow = {
    match_id: matchId,
    platform: derivePlatform(matchId),
    game_creation: new Date(info.gameCreation).toISOString(),
    game_duration_seconds: robustDuration(info),
    game_version: info.gameVersion ?? null,
    queue_id: info.queueId ?? null,
  };

  // Extract dragon soul info from teams
  let dragonSoulType: string | null = null;
  let dragonSoulTeamId: number | null = null;
  for (const t of info.teams ?? []) {
    // Riot API: if a team got 4+ dragons, they got the soul
    const dragonKills = t.objectives?.dragon?.kills ?? 0;
    if (dragonKills >= 4) {
      dragonSoulTeamId = t.teamId;
      // The soul type is in the timeline events, but we can check objectives
      // Riot doesn't expose soul type directly in match DTO — we extract from timeline later
    }
  }

  const teamRows = (info.teams ?? []).map((t: any) => ({
    match_id: matchId,
    team_id: t.teamId,
    win: Boolean(t.win),
    first_dragon: t.objectives?.dragon?.first ?? null,
    first_baron: t.objectives?.baron?.first ?? null,
    towers_destroyed: t.objectives?.tower?.kills ?? null,
    dragons: t.objectives?.dragon?.kills ?? null,
    barons: t.objectives?.baron?.kills ?? null,
  }));

  // Compute team totals for kill participation & damage share
  const teamKills = new Map<number, number>();
  const teamDamage = new Map<number, number>();
  for (const p of info.participants ?? []) {
    const tid = p.teamId ?? 0;
    teamKills.set(tid, (teamKills.get(tid) ?? 0) + (p.kills ?? 0));
    teamDamage.set(tid, (teamDamage.get(tid) ?? 0) + (p.totalDamageDealtToChampions ?? 0));
  }

  const partRows = (info.participants ?? []).map((p: any) => {
    const tid = p.teamId ?? 0;
    const tk = teamKills.get(tid) ?? 1;
    const td = teamDamage.get(tid) ?? 1;
    const kp = tk > 0 ? ((p.kills ?? 0) + (p.assists ?? 0)) / tk : 0;
    const ds = td > 0 ? (p.totalDamageDealtToChampions ?? 0) / td : 0;

    return {
      match_id: matchId,
      participant_id: p.participantId,
      puuid: p.puuid ?? null,
      summoner_name: p.riotIdGameName ?? p.summonerName ?? null,
      team_id: tid,
      champion_id: p.championId ?? null,
      champion_name: p.championName ?? null,
      role: p.teamPosition || p.individualPosition || null,
      lane: p.lane ?? null,
      win: Boolean(p.win),
      kills: p.kills ?? 0,
      deaths: p.deaths ?? 0,
      assists: p.assists ?? 0,
      gold_earned: p.goldEarned ?? 0,
      total_damage_to_champions: p.totalDamageDealtToChampions ?? 0,
      vision_score: p.visionScore ?? 0,
      item0: p.item0 ?? 0,
      item1: p.item1 ?? 0,
      item2: p.item2 ?? 0,
      item3: p.item3 ?? 0,
      item4: p.item4 ?? 0,
      item5: p.item5 ?? 0,
      item6: p.item6 ?? 0,
      perk_primary_style: p.perks?.styles?.[0]?.style ?? null,
      perk_sub_style: p.perks?.styles?.[1]?.style ?? null,
      perk_keystone: p.perks?.styles?.[0]?.selections?.[0]?.perk ?? null,
      // New fields
      total_cs: (p.totalMinionsKilled ?? 0) + (p.neutralMinionsKilled ?? 0),
      total_minions_killed: p.totalMinionsKilled ?? 0,
      neutral_minions_killed: p.neutralMinionsKilled ?? 0,
      champ_level: p.champLevel ?? 0,
      time_played: p.timePlayed ?? 0,
      solo_kills: p.challenges?.soloKills ?? 0,
      summoner1_id: p.summoner1Id ?? 0,
      summoner2_id: p.summoner2Id ?? 0,
      kill_participation: Math.round(kp * 1000) / 1000,
      damage_share: Math.round(ds * 1000) / 1000,
      riot_id_game_name: p.riotIdGameName ?? null,
      riot_id_tagline: p.riotIdTagline ?? null,
    };
  });

  const matchRegionStr = platformToRegion(derivePlatform(matchId));
  const discoveredUsers = matchRegionStr
    ? (info.participants ?? [])
        .filter((p: any) => p.puuid && p.riotIdGameName && p.riotIdTagline)
        .map((p: any) => ({
          puuid: p.puuid,
          name: p.riotIdGameName,
          tag: p.riotIdTagline,
          icon_id: p.profileIcon ?? 0,
          region: matchRegionStr.toUpperCase(),
        }))
    : [];

  // ── Fire ALL DB writes + timeline fetch in parallel ──

  // ── Fire DB writes + timeline fetch ALL in parallel ──
  const matchRegion = platformToRegion(derivePlatform(matchId)) as Region | null;
  const timelinePromise = matchRegion ? getMatchTimeline(matchId, matchRegion).catch(() => null) : Promise.resolve(null);

  const dbOps: Promise<any>[] = [
    supabase.from("matches").upsert(matchRow, { onConflict: "match_id" }),
    teamRows.length > 0
      ? supabase.from("match_teams").upsert(teamRows, { onConflict: "match_id,team_id" })
      : Promise.resolve(null),
    partRows.length > 0
      ? supabase.from("participants").upsert(partRows, { onConflict: "match_id,participant_id" })
      : Promise.resolve(null),
    discoveredUsers.length > 0
      ? upsertUsers(discoveredUsers)
      : Promise.resolve(null),
  ];

  const dbResults = await Promise.all([...dbOps, timelinePromise]);
  const matchUpsertResult = dbResults[0];
  const timeline = dbResults[4];

  // Skip timeline if match upsert failed (foreign key would break)
  if (matchUpsertResult?.error) {
    return;
  }

  // ── Timeline ingestion (item events + early game stats + dragon soul) ──
  try {
    if (timeline?.info?.frames) {
      // 1. Extract item purchase events (participantId must be 1-10)
      const itemEvents: any[] = [];
      for (const frame of timeline.info.frames) {
        for (const event of frame.events ?? []) {
          if (event.type === "ITEM_PURCHASED" && event.itemId && event.participantId >= 1 && event.participantId <= 10) {
            const participant = partRows.find((p: any) => p.participant_id === event.participantId);
            itemEvents.push({
              match_id: matchId,
              participant_id: event.participantId,
              puuid: participant?.puuid ?? null,
              ts_ms: event.timestamp,
              event_type: "PURCHASE",
              item_id: event.itemId,
              gold: null,
            });
          }
        }
      }

      if (itemEvents.length > 0) {
        for (let i = 0; i < itemEvents.length; i += 500) {
          const batch = itemEvents.slice(i, i + 500);
          const { error } = await supabase.from("participant_item_events")
            .upsert(batch, { onConflict: "match_id,participant_id,ts_ms,event_type,item_id", ignoreDuplicates: true });
          if (error) log.warn("TIMELINE", `Item events upsert error: ${error.message?.slice(0, 100)}`);
        }
      }

      // 2. Extract early game stats
      const earlyStats = extractEarlyGameStats(timeline, partRows.map((p: any) => p.participant_id));
      if (Object.keys(earlyStats).length > 0) {
        const earlyUpdates = Object.entries(earlyStats).map(([pidStr, stats]) => {
          const pid = Number(pidStr);
          return supabase.from("participants")
            .update({
              gold_at_10: stats.gold_at_10,
              cs_at_10: stats.cs_at_10,
              xp_at_10: stats.xp_at_10,
              damage_at_10: stats.damage_at_10,
              kills_at_10: stats.kills_at_10,
              deaths_at_10: stats.deaths_at_10,
              assists_at_10: stats.assists_at_10,
            })
            .eq("match_id", matchId)
            .eq("participant_id", pid);
        });
        await Promise.all(earlyUpdates);
      }

      // 3. Extract dragon soul type from timeline events
      for (const frame of timeline.info.frames) {
        for (const event of frame.events ?? []) {
          if (event.type === "ELITE_MONSTER_KILL" && event.monsterType === "DRAGON" && event.monsterSubType) {
            const subType = event.monsterSubType as string;
            if (subType.includes("ELDER")) continue;
            const soulMap: Record<string, string> = {
              "FIRE_DRAGON": "Infernal",
              "WATER_DRAGON": "Ocean",
              "EARTH_DRAGON": "Mountain",
              "AIR_DRAGON": "Cloud",
              "HEXTECH_DRAGON": "Hextech",
              "CHEMTECH_DRAGON": "Chemtech",
            };
            const soulName = soulMap[subType];
            if (soulName && dragonSoulTeamId) {
              await supabase.from("match_teams")
                .update({ dragon_soul: soulName, dragon_soul_team_id: dragonSoulTeamId })
                .eq("match_id", matchId)
                .eq("team_id", dragonSoulTeamId);
              break;
            }
          }
        }
      }
    }
  } catch (timelineErr: any) {
    log.warn("TIMELINE", `Timeline ingestion failed for ${matchId}: ${timelineErr?.message?.slice(0, 100)}`);
  }
}

function queuesFor(group: string): number[] {
  if (group === "ranked_solo") return [Q_SOLO];
  if (group === "ranked_flex") return [Q_FLEX];
  return [Q_SOLO, Q_FLEX]; // ranked_all
}

async function updatePlayerSeasonStats(puuid: string, region: Region): Promise<number> {
  const queueGroup = "ranked_all";
  const seasonStart = SEASON_START_EPOCH;
  const seasonEnd = SEASON_END_EPOCH ?? null;
  const queues = queuesFor(queueGroup);

  // Ensure aggregate row exists
  await supabase.from("season_aggregates").upsert(
    {
      puuid,
      region,
      season_start: seasonStart,
      season_end: seasonEnd,
      queue_group: queueGroup,
      status: "backfilling",
    },
    { onConflict: "puuid,season_start,queue_group" },
  );

  // ── Collect ALL match IDs for the season (no early stopping) ──
  const allIds: string[] = [];

  for (const q of queues) {
    if (isBadPuuid(puuid)) break;
    let start = 0;

    while (allIds.length < MAX_NEW_MATCHES) {
      const count = Math.min(100, MAX_NEW_MATCHES - allIds.length);

      const ids = await getMatchIds(puuid, region, {
        start,
        count,
        queue: q,
        type: "ranked",
        startTime: seasonStart || undefined,
        endTime: seasonEnd ?? undefined,
      });

      if (!ids?.length) break;
      allIds.push(...ids);
      if (ids.length < 100) break;
      start += ids.length;
    }

    if (allIds.length >= MAX_NEW_MATCHES) break;
  }

  if (allIds.length === 0) {
    await supabase
      .from("season_aggregates")
      .update({
        last_scan_at: new Date().toISOString(),
        computed_at: new Date().toISOString(),
        status: "ok",
      })
      .eq("puuid", puuid)
      .eq("season_start", seasonStart)
      .eq("queue_group", queueGroup);
    return 0;
  }

  // Filter out already-processed matches in bulk
  const { data: existing, error } = await supabase
    .from("season_processed_matches")
    .select("match_id")
    .eq("puuid", puuid)
    .eq("season_start", seasonStart)
    .eq("queue_group", queueGroup)
    .in("match_id", allIds);

  if (error) {
    log.error("SEASON_ERR", "Failed to check processed matches", {
      puuid: puuid.slice(0, 12),
      message: error.message,
    });
    return 0;
  }

  const seen = new Set((existing ?? []).map((r: { match_id: string }) => r.match_id));
  const newIds = allIds.filter((id) => !seen.has(id));

  if (newIds.length === 0) {
    await supabase
      .from("season_aggregates")
      .update({
        last_scan_at: new Date().toISOString(),
        computed_at: new Date().toISOString(),
        status: "ok",
      })
      .eq("puuid", puuid)
      .eq("season_start", seasonStart)
      .eq("queue_group", queueGroup);
    return 0;
  }

  // Use the cached region (might differ from DB if fallback was used)
  const matchRegion = getCachedRegion(puuid) ?? region;

  // Process matches in parallel batches
  const MATCH_BATCH = 8;
  let processed = 0;
  const sortedNewIds = newIds.reverse();

  for (let b = 0; b < sortedNewIds.length; b += MATCH_BATCH) {
    const batch = sortedNewIds.slice(b, b + MATCH_BATCH);
    const matchResults = await Promise.all(
      batch.map(async (matchId) => {
        const match = await getMatchDetails(matchId, matchRegion);
        if (!match) return null;
        const queueId = match.info?.queueId;
        if (!queues.includes(queueId)) return null;
        await ingestMatch(match, matchRegion);
        return { matchId, match, queueId };
      })
    );

    for (const result of matchResults) {
      if (!result) continue;
      const { matchId, match, queueId } = result;

    const me = match.info.participants?.find((p: any) => p.puuid === puuid);
    if (!me) continue;

    const champ: string = me.championName ?? "Unknown";
    const cs: number = (me.totalMinionsKilled ?? 0) + (me.neutralMinionsKilled ?? 0);
    const mins: number = (match.info.gameDuration ?? 0) / 60;
    const gameStart: number = match.info.gameStartTimestamp ?? match.info.gameCreation ?? 0;

    // Insert processed match record (unique constraint protects against dupes)
    const { error: insErr } = await supabase.from("season_processed_matches").insert({
      puuid,
      region,
      season_start: seasonStart,
      queue_group: queueGroup,
      match_id: matchId,
      game_start: gameStart,
      queue_id: queueId,
    });

    if (insErr) {
      const code = (insErr as any).code;
      const msg = String((insErr as any).message ?? "").toLowerCase();
      if (code === "23505" || msg.includes("duplicate")) continue;
      log.error("SEASON_INSERT", `Failed: ${insErr.message}`, { matchId });
      continue;
    }

    // Apply totals delta (atomic RPC)
    await supabase.rpc("season_apply_delta", {
      p_puuid: puuid,
      p_season_start: seasonStart,
      p_queue_group: queueGroup,
      p_region: region,
      p_games: 1,
      p_wins: me.win ? 1 : 0,
      p_gold: me.goldEarned ?? 0,
      p_kills: me.kills ?? 0,
      p_deaths: me.deaths ?? 0,
      p_assists: me.assists ?? 0,
      p_cs: cs,
      p_duration_min: mins,
      p_last_game_start: gameStart,
    });

    // Apply champion delta (atomic RPC)
    await supabase.rpc("season_apply_champion_delta", {
      p_puuid: puuid,
      p_season_start: seasonStart,
      p_queue_group: queueGroup,
      p_region: region,
      p_champion: champ,
      p_games: 1,
      p_wins: me.win ? 1 : 0,
      p_gold: me.goldEarned ?? 0,
      p_kills: me.kills ?? 0,
      p_deaths: me.deaths ?? 0,
      p_assists: me.assists ?? 0,
      p_cs: cs,
      p_duration_min: mins,
    });

    // Apply matchup delta (find lane opponent)
    const myRole = me.teamPosition || me.individualPosition || "";
    if (myRole) {
      const enemyTeamId = me.teamId === 100 ? 200 : 100;
      const laneOpponent = match.info.participants.find(
        (p: any) =>
          p.teamId === enemyTeamId &&
          (p.teamPosition || p.individualPosition || "") === myRole,
      );
      if (laneOpponent) {
        try {
          await supabase.rpc("season_apply_matchup_delta", {
            p_puuid: puuid,
            p_season_start: seasonStart,
            p_queue_group: queueGroup,
            p_region: region,
            p_champion: champ,
            p_opponent: laneOpponent.championName ?? "Unknown",
            p_games: 1,
            p_wins: me.win ? 1 : 0,
            p_kills: me.kills ?? 0,
            p_deaths: me.deaths ?? 0,
            p_assists: me.assists ?? 0,
          });
        } catch {
          // non-fatal if table/function doesn't exist yet
        }
      }
    }

      processed++;
    }
  }

  // Mark completed
  await supabase
    .from("season_aggregates")
    .update({
      last_scan_at: new Date().toISOString(),
      computed_at: new Date().toISOString(),
      status: "ok",
    })
    .eq("puuid", puuid)
    .eq("season_start", seasonStart)
    .eq("queue_group", queueGroup);

  return processed;
}

export async function runUpdateSeasonStats(opts?: { masterPlusOnly?: boolean }): Promise<void> {
  const startTime = Date.now();
  log.info("JOB_START", "update-season-stats started (Master+ EUW only)");

  // Fetch ladder from Riot API (sorted by LP, highest first)
  let ladderPuuids: string[] = [];
  try {
    const { getChallenger, getGrandmaster, getMaster } = await import("../riot");
    const [chall, gm, master] = await Promise.all([
      getChallenger("RANKED_SOLO_5x5", "EUW" as any),
      getGrandmaster("RANKED_SOLO_5x5", "EUW" as any),
      getMaster("RANKED_SOLO_5x5", "EUW" as any),
    ]);
    const all = [...chall, ...gm, ...master]
      .sort((a: any, b: any) => (b.leaguePoints ?? 0) - (a.leaguePoints ?? 0));
    ladderPuuids = all.map((e: any) => e.puuid).filter(Boolean);
    log.info("LADDER", `Fetched ${ladderPuuids.length} players from Riot ladder (sorted by LP)`);
  } catch (e: any) {
    log.warn("LADDER", `Failed to fetch ladder: ${e.message?.slice(0, 80)}`);
  }

  // Get all Master+ users from DB
  const dbUsers = await getMasterPlusUserPuuids();

  // Merge: ladder players first (by LP), then remaining DB users
  const seen = new Set<string>();
  const users: { puuid: string; region: string }[] = [];
  for (const puuid of ladderPuuids) {
    if (!seen.has(puuid)) {
      seen.add(puuid);
      users.push({ puuid, region: "EUW" });
    }
  }
  for (const u of dbUsers) {
    if (!seen.has(u.puuid)) {
      seen.add(u.puuid);
      users.push(u);
    }
  }

  log.info("SEASON", `Processing ${users.length} users (ladder-first order)`);

  let totalProcessed = 0;
  let errors = 0;
  let skippedBad = 0;

  const BATCH = 10; // Process 10 players concurrently

  for (let i = 0; i < users.length; i += BATCH) {
    const batch = users.slice(i, i + BATCH).filter(({ puuid }) => {
      if (isBadPuuid(puuid)) { skippedBad++; return false; }
      return true;
    });

    if (batch.length === 0) continue;

    const results = await Promise.allSettled(
      batch.map(({ puuid, region }) =>
        updatePlayerSeasonStats(puuid.trim(), region as Region),
      ),
    );

    for (const result of results) {
      if (result.status === "fulfilled") {
        totalProcessed += result.value;
      } else {
        errors++;
        const msg = result.reason instanceof Error ? result.reason.message : String(result.reason);
        log.error("SEASON_PLAYER_ERR", `Failed: ${msg}`);
      }
    }

    if (i > 0 && i % 50 === 0) {
      const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      log.info("SEASON_PROGRESS", `${i}/${users.length} users | ${totalProcessed} matches | ${skippedBad} bad | ${errors} err | ${elapsed}min`);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  log.info("JOB_END", `update-season-stats completed in ${elapsed}min`, {
    users: users.length,
    matchesProcessed: totalProcessed,
    errors,
  });
}

// Auto-run when executed directly
runUpdateSeasonStats().catch(console.error);
