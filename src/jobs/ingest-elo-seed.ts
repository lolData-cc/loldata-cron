/**
 * Automatic elo-branch seeding.
 *
 * Fetches players from Diamond through Gold (configurable) for all
 * regions and queues, resolves their account info, and upserts to
 * the users table so that update-season-stats processes their matches.
 *
 * Each bracket costs 1 API call to list + 1 call per player to resolve
 * gameName/tagLine.  At 200 players × 96 brackets = ~19,200 API calls
 * (~7 min at 490/10s).
 */

import {
  REGIONS,
  QUEUE_SOLO,
  QUEUE_FLEX,
  CONCURRENCY,
  SEED_TIERS,
  DIVISIONS,
  SEED_SAMPLE_PER_BRACKET,
} from "../config";
import type { Region } from "../config";
import type { QueueApi, LeagueEntryDTO } from "../types";
import { getLeagueEntries, getAccountByPuuid } from "../riot";
import { upsertUsers, formatRank } from "../db";
import { log } from "../logger";

// ── Fetch a bracket page by page until we have enough entries ────────

async function fetchBracketEntries(
  queue: QueueApi,
  tier: string,
  division: string,
  region: Region,
  maxPlayers: number,
): Promise<LeagueEntryDTO[]> {
  const collected: LeagueEntryDTO[] = [];
  let page = 1;

  while (collected.length < maxPlayers) {
    const entries = await getLeagueEntries(queue, tier, division, page, region);
    if (entries.length === 0) break;

    collected.push(...entries);
    page++;

    // Safety: cap at 5 pages per bracket (~1025 entries max)
    if (page > 5) break;
  }

  return collected.slice(0, maxPlayers);
}

// ── Resolve a single league entry → user row for upsert ─────────────

async function resolveEloEntry(
  entry: LeagueEntryDTO,
  region: Region,
  queue: QueueApi,
): Promise<Record<string, unknown> | null> {
  const puuid = entry.puuid;
  if (!puuid) return null;

  // 1 API call: resolve gameName + tagLine (required by getAllUserPuuids filter)
  const account = await getAccountByPuuid(puuid, region);
  if (!account?.gameName || !account?.tagLine) return null;

  const rankStr = formatRank(entry.tier, entry.rank);

  const row: Record<string, unknown> = {
    puuid,
    name: account.gameName,
    tag: account.tagLine,
    icon_id: 0, // Skip icon resolution to save API budget
    region: region.toUpperCase(),
  };

  if (queue === QUEUE_SOLO) {
    row.rank = rankStr;
  } else {
    row.flex_rank = rankStr;
    row.flex_lp = entry.leaguePoints;
  }

  return row;
}

// ── Batch resolve & upsert (mirrors ingest-ladder.ts pattern) ───────

async function resolveAndUpsertBatch(
  entries: LeagueEntryDTO[],
  region: Region,
  queue: QueueApi,
): Promise<number> {
  let resolved = 0;
  const upsertRows: Record<string, unknown>[] = [];

  for (let i = 0; i < entries.length; i += CONCURRENCY) {
    const batch = entries.slice(i, i + CONCURRENCY);

    const results = await Promise.allSettled(
      batch.map((entry) => resolveEloEntry(entry, region, queue)),
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

    // Progress logging every 500 entries
    if (i > 0 && i % 500 === 0) {
      log.info("ELO_SEED_PROGRESS", `${region} ${queue} ${entries[0]?.tier ?? "?"}: ${i}/${entries.length} resolved`);
    }
  }

  // Flush remaining
  if (upsertRows.length > 0) {
    await upsertUsers(upsertRows);
  }

  return resolved;
}

// ── Main exported job ───────────────────────────────────────────────

export async function runIngestEloSeed(): Promise<void> {
  const startTime = Date.now();
  log.info("JOB_START", "ingest-elo-seed started", {
    tiers: SEED_TIERS.join(","),
    samplePerBracket: SEED_SAMPLE_PER_BRACKET,
  });

  let totalResolved = 0;
  let totalBrackets = 0;

  for (const region of REGIONS) {
    for (const queue of [QUEUE_SOLO, QUEUE_FLEX] as const) {
      for (const tier of SEED_TIERS) {
        for (const division of DIVISIONS) {
          totalBrackets++;
          try {
            const entries = await fetchBracketEntries(
              queue,
              tier,
              division,
              region,
              SEED_SAMPLE_PER_BRACKET,
            );

            if (entries.length === 0) {
              log.info("ELO_SEED", `${region} ${queue} ${tier} ${division}: empty bracket`);
              continue;
            }

            const resolved = await resolveAndUpsertBatch(entries, region, queue);
            totalResolved += resolved;

            log.info("ELO_SEED_DONE", `${region} ${queue} ${tier} ${division}: resolved ${resolved}/${entries.length}`);
          } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err);
            log.error("ELO_SEED_ERR", `Failed ${region} ${queue} ${tier} ${division}: ${msg}`);
          }
        }
      }
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  log.info("JOB_END", `ingest-elo-seed completed in ${elapsed}min`, {
    totalResolved,
    totalBrackets,
  });
}

runIngestEloSeed().catch(console.error);
