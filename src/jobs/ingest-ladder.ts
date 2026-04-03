import { REGIONS, QUEUE_SOLO, QUEUE_FLEX, CONCURRENCY } from "../config";
import type { Region } from "../config";
import type { QueueApi, LadderEntry } from "../types";
import {
  getChallenger,
  getGrandmaster,
  getMaster,
  getSummonerByEncryptedId,
  getSummonerByPuuid,
  getAccountByPuuid,
} from "../riot";
import { upsertUsers, formatRank } from "../db";
import { log } from "../logger";

async function fetchFullLadder(queue: QueueApi, region: Region): Promise<LadderEntry[]> {
  log.info("LADDER", `Fetching ${queue} ladder for ${region}`);

  const [challengers, grandmasters, masters] = await Promise.all([
    getChallenger(queue, region),
    getGrandmaster(queue, region),
    getMaster(queue, region),
  ]);

  const all = [...challengers, ...grandmasters, ...masters];
  log.info("LADDER", `${region} ${queue}: ${all.length} entries`, {
    challengers: challengers.length,
    grandmasters: grandmasters.length,
    masters: masters.length,
  });

  return all;
}

async function resolveEntry(
  entry: LadderEntry,
  region: Region,
  queue: QueueApi,
): Promise<Record<string, unknown> | null> {
  let puuid = entry.puuid ?? null;
  let profileIconId: number | null = null;

  // Resolve puuid from summonerId if needed
  if (!puuid && entry.summonerId) {
    const summoner = await getSummonerByEncryptedId(entry.summonerId, region);
    if (!summoner) return null;
    puuid = summoner.puuid;
    profileIconId = summoner.profileIconId;
  }

  if (!puuid) return null;

  // Get profileIconId if we don't have it yet
  if (profileIconId == null) {
    const summoner = await getSummonerByPuuid(puuid, region);
    profileIconId = summoner?.profileIconId ?? 0;
  }

  // Resolve gameName + tagLine
  const account = await getAccountByPuuid(puuid, region);
  if (!account?.gameName || !account?.tagLine) return null;

  const rankStr = formatRank(entry.tier, undefined); // Apex tiers have no division

  const row: Record<string, unknown> = {
    puuid,
    name: account.gameName,
    tag: account.tagLine,
    icon_id: profileIconId ?? 0,
    region: region.toUpperCase(),
  };

  // Set the appropriate rank field based on queue type
  if (queue === QUEUE_SOLO) {
    row.rank = rankStr;
  } else {
    row.flex_rank = rankStr;
    row.flex_lp = entry.leaguePoints;
  }

  return row;
}

async function resolveAndUpsertBatch(
  entries: LadderEntry[],
  region: Region,
  queue: QueueApi,
): Promise<number> {
  let resolved = 0;
  const upsertRows: Record<string, unknown>[] = [];

  for (let i = 0; i < entries.length; i += CONCURRENCY) {
    const batch = entries.slice(i, i + CONCURRENCY);

    const results = await Promise.allSettled(
      batch.map((entry) => resolveEntry(entry, region, queue)),
    );

    for (const result of results) {
      if (result.status === "fulfilled" && result.value) {
        upsertRows.push(result.value);
        resolved++;
      }
    }

    // Upsert in chunks to avoid memory buildup
    if (upsertRows.length >= 200) {
      await upsertUsers(upsertRows.splice(0, 200));
    }

    // Progress logging
    if (i > 0 && i % 500 === 0) {
      log.info("LADDER_PROGRESS", `${region} ${queue}: ${i}/${entries.length} resolved`);
    }
  }

  // Flush remaining
  if (upsertRows.length > 0) {
    await upsertUsers(upsertRows);
  }

  return resolved;
}

export async function runIngestLadder(): Promise<void> {
  const startTime = Date.now();
  log.info("JOB_START", "ingest-ladder started");

  let totalResolved = 0;

  for (const region of REGIONS) {
    for (const queue of [QUEUE_SOLO, QUEUE_FLEX] as const) {
      try {
        const entries = await fetchFullLadder(queue, region);
        const resolved = await resolveAndUpsertBatch(entries, region, queue);
        totalResolved += resolved;
        log.info("LADDER_DONE", `${region} ${queue}: resolved ${resolved}/${entries.length}`);
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        log.error("LADDER_ERR", `Failed ${region} ${queue}: ${msg}`);
      }
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  log.info("JOB_END", `ingest-ladder completed in ${elapsed}min`, { totalResolved });
}
