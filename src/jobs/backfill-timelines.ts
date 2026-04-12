/**
 * Backfill timeline data (item events + early game stats + dragon soul)
 * for existing matches that don't have timeline data yet.
 *
 * Runs through matches in batches, fetching timelines from Riot API.
 * Rate limited to avoid hitting API limits.
 */
import { config } from "dotenv";
config({ override: true });

import { RIOT_API_KEY, REGION_ROUTING, RIOT_LIMIT_10S } from "../config";
import type { Region } from "../config";
import { supabase } from "../db";
import { getMatchTimeline } from "../riot";
import { log } from "../logger";

const BATCH_SIZE = 100;
const CONCURRENCY = 5;
const DELAY_BETWEEN_BATCHES_MS = 2000;

function derivePlatform(matchId: string): string {
  return matchId.split("_")[0]?.toLowerCase() ?? "";
}

const PLATFORM_TO_REGION: Record<string, Region> = {
  euw1: "EUW",
  na1: "NA",
  kr: "KR",
};

async function processMatch(matchId: string): Promise<{ items: number; early: boolean; soul: string | null }> {
  const platform = derivePlatform(matchId);
  const region = PLATFORM_TO_REGION[platform];
  if (!region) return { items: 0, early: false, soul: null };

  const timeline = await getMatchTimeline(matchId, region);
  if (!timeline?.info?.frames) return { items: 0, early: false, soul: null };

  // Get participant mapping
  const { data: participants } = await supabase
    .from("participants")
    .select("participant_id, puuid, team_id")
    .eq("match_id", matchId);

  const pidToPuuid = new Map<number, string>();
  const pidToTeam = new Map<number, number>();
  for (const p of participants ?? []) {
    pidToPuuid.set(p.participant_id, p.puuid);
    pidToTeam.set(p.participant_id, p.team_id);
  }

  // 1. Extract item purchase events
  const itemEvents: any[] = [];
  for (const frame of timeline.info.frames) {
    for (const event of frame.events ?? []) {
      if (event.type === "ITEM_PURCHASED" && event.itemId) {
        itemEvents.push({
          match_id: matchId,
          participant_id: event.participantId,
          puuid: pidToPuuid.get(event.participantId) ?? null,
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
      await supabase.from("participant_item_events").insert(batch);
    }
  }

  // 2. Early game stats
  let earlyDone = false;
  const TEN_MIN = 600_000;
  let bestFrame: any = null;
  let bestDiff = Infinity;
  for (const frame of timeline.info.frames) {
    const diff = Math.abs(frame.timestamp - TEN_MIN);
    if (diff < bestDiff) {
      bestDiff = diff;
      bestFrame = frame;
    }
  }

  if (bestFrame?.participantFrames) {
    const updates: Promise<any>[] = [];
    // Count kills/deaths/assists before 10 min
    const killsBefore10: Record<number, { k: number; d: number; a: number }> = {};
    for (const frame of timeline.info.frames) {
      if (frame.timestamp > TEN_MIN) break;
      for (const event of frame.events ?? []) {
        if (event.type === "CHAMPION_KILL") {
          const killer = event.killerId ?? 0;
          const victim = event.victimId ?? 0;
          const assists = event.assistingParticipantIds ?? [];
          if (killer > 0) {
            if (!killsBefore10[killer]) killsBefore10[killer] = { k: 0, d: 0, a: 0 };
            killsBefore10[killer].k++;
          }
          if (victim > 0) {
            if (!killsBefore10[victim]) killsBefore10[victim] = { k: 0, d: 0, a: 0 };
            killsBefore10[victim].d++;
          }
          for (const a of assists) {
            if (!killsBefore10[a]) killsBefore10[a] = { k: 0, d: 0, a: 0 };
            killsBefore10[a].a++;
          }
        }
      }
    }

    for (const [pidStr, pf] of Object.entries(bestFrame.participantFrames)) {
      const pid = Number(pidStr);
      const kda = killsBefore10[pid] ?? { k: 0, d: 0, a: 0 };
      updates.push(
        supabase.from("participants")
          .update({
            gold_at_10: (pf as any).totalGold ?? null,
            cs_at_10: ((pf as any).minionsKilled ?? 0) + ((pf as any).jungleMinionsKilled ?? 0),
            xp_at_10: (pf as any).xp ?? null,
            damage_at_10: (pf as any).damageStats?.totalDamageDoneToChampions ?? 0,
            kills_at_10: kda.k,
            deaths_at_10: kda.d,
            assists_at_10: kda.a,
          })
          .eq("match_id", matchId)
          .eq("participant_id", pid)
      );
    }
    await Promise.all(updates);
    earlyDone = true;
  }

  // 3. Dragon soul
  let soulType: string | null = null;
  const soulMap: Record<string, string> = {
    FIRE_DRAGON: "Infernal",
    WATER_DRAGON: "Ocean",
    EARTH_DRAGON: "Mountain",
    AIR_DRAGON: "Cloud",
    HEXTECH_DRAGON: "Hextech",
    CHEMTECH_DRAGON: "Chemtech",
  };

  // Count dragons per team to find who got soul
  const dragonsByTeam: Record<number, { count: number; types: string[] }> = {};
  for (const frame of timeline.info.frames) {
    for (const event of frame.events ?? []) {
      if (event.type === "ELITE_MONSTER_KILL" && event.monsterType === "DRAGON") {
        const subType = event.monsterSubType as string;
        if (subType?.includes("ELDER")) continue;
        const killerTeam = pidToTeam.get(event.killerId) ?? 0;
        if (killerTeam) {
          if (!dragonsByTeam[killerTeam]) dragonsByTeam[killerTeam] = { count: 0, types: [] };
          dragonsByTeam[killerTeam].count++;
          if (soulMap[subType]) dragonsByTeam[killerTeam].types.push(subType);
        }
      }
    }
  }

  for (const [teamIdStr, data] of Object.entries(dragonsByTeam)) {
    if (data.count >= 4 && data.types.length > 0) {
      // The 4th dragon's type determines the soul
      soulType = soulMap[data.types[0]] ?? null;
      const teamId = Number(teamIdStr);
      await supabase.from("match_teams")
        .update({ dragon_soul: soulType, dragon_soul_team_id: teamId })
        .eq("match_id", matchId)
        .eq("team_id", teamId);
    }
  }

  return { items: itemEvents.length, early: earlyDone, soul: soulType };
}

async function main() {
  log.info("BACKFILL", "Starting timeline backfill...");
  const startTime = Date.now();

  let totalProcessed = 0;
  let totalItems = 0;
  let totalEarly = 0;
  let totalSoul = 0;
  let offset = 0;

  while (true) {
    // Get matches that don't have item events yet
    const { data: matches, error } = await supabase
      .from("matches")
      .select("match_id")
      .order("game_creation", { ascending: false })
      .range(offset, offset + BATCH_SIZE - 1);

    if (error || !matches?.length) {
      log.info("BACKFILL", `No more matches at offset ${offset}`);
      break;
    }

    // Filter out matches that already have item events
    const matchIds = matches.map(m => m.match_id);
    const { data: existing } = await supabase
      .from("participant_item_events")
      .select("match_id")
      .in("match_id", matchIds);

    const existingSet = new Set((existing ?? []).map(e => e.match_id));
    const toProcess = matchIds.filter(id => !existingSet.has(id));

    if (toProcess.length === 0) {
      offset += BATCH_SIZE;
      if (offset % 1000 === 0) log.info("BACKFILL", `Skipped to offset ${offset} (already processed)`);
      continue;
    }

    // Process in parallel batches
    for (let i = 0; i < toProcess.length; i += CONCURRENCY) {
      const batch = toProcess.slice(i, i + CONCURRENCY);
      const results = await Promise.allSettled(batch.map(processMatch));

      for (const r of results) {
        if (r.status === "fulfilled") {
          totalItems += r.value.items;
          if (r.value.early) totalEarly++;
          if (r.value.soul) totalSoul++;
        }
      }
      totalProcessed += batch.length;

      // Rate limit delay
      await new Promise(r => setTimeout(r, 200));
    }

    offset += BATCH_SIZE;

    if (totalProcessed % 100 === 0) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      log.info("BACKFILL", `Processed ${totalProcessed} matches in ${elapsed}s — ${totalItems} items, ${totalEarly} early stats, ${totalSoul} souls`);
    }

    await new Promise(r => setTimeout(r, DELAY_BETWEEN_BATCHES_MS));
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
  log.info("BACKFILL", `Done! ${totalProcessed} matches in ${elapsed}s — ${totalItems} items, ${totalEarly} early stats, ${totalSoul} souls`);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    log.error("BACKFILL", `Fatal: ${err.message}`);
    process.exit(1);
  });
