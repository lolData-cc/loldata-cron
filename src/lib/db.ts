// src/lib/db.ts
//
// Data layer: Supabase (PostgREST) for upserts the jobs do, plus a raw
// `pg` pool for the analytics/snapshot SQL (group-bys that PostgREST can't
// express). Also the global match-dedup + the ingest worklist queries.

import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { Pool } from "pg";
import { SUPABASE_URL, SUPABASE_KEY, DATABASE_URL } from "../config";
import { log } from "./logger";

export const supa: SupabaseClient = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

let _pool: Pool | null = null;
export function pool(): Pool {
  if (!_pool) {
    if (!DATABASE_URL) throw new Error("DATABASE_URL not set (needed for analytics SQL)");
    _pool = new Pool({ connectionString: DATABASE_URL, max: 8 });
  }
  return _pool;
}

function chunk<T>(arr: T[], n: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

// ── players (autocomplete index + ingest worklist + tier source) ───
export type PlayerUpsert = {
  puuid: string;
  game_name: string | null;
  tag_line: string | null;
  platform: string; // e.g. "EUW1"
  region: string; // routing, e.g. "europe"
  tier: string | null; // solo tier
  division: string | null;
  lp: number | null;
  flex_tier?: string | null;
  flex_lp?: number | null;
  icon_id?: number | null;
  summoner_level?: number | null;
};

export async function upsertPlayers(rows: PlayerUpsert[]): Promise<void> {
  if (rows.length === 0) return;
  for (const c of chunk(rows, 500)) {
    const { error } = await supa
      .from("players")
      .upsert(
        c.map((r) => ({ ...r, last_crawled_at: new Date().toISOString() })),
        { onConflict: "puuid" }
      );
    if (error) log.error("players.upsert:", error.message);
  }
}

/** Mark a set of players as freshly ingested (so the worklist rotates). */
export async function markIngested(puuids: string[]): Promise<void> {
  if (puuids.length === 0) return;
  for (const c of chunk(puuids, 500)) {
    const { error } = await supa
      .from("players")
      .update({ last_ingest_at: new Date().toISOString() })
      .in("puuid", c);
    if (error) log.error("players.markIngested:", error.message);
  }
}

/** Worklist: players to ingest next, least-recently ingested first (nulls,
 *  i.e. never-ingested, first), then highest LP. `tiers` filters (e.g. apex).
 *
 *  Uses the raw `pg` pool, NOT PostgREST: Supabase caps responses at
 *  `max-rows` (1000 by default), which would silently truncate the worklist
 *  to 1000 players regardless of `limit`. The whole apex base is ~17k, so we
 *  must bypass that cap to cover everyone. */
export async function getIngestWorklist(
  platform: string,
  tiers: string[],
  limit: number
): Promise<{ puuid: string; tier: string | null }[]> {
  const params: unknown[] = [platform];
  let where = "platform = $1";
  if (tiers.length > 0) {
    params.push(tiers);
    where += ` AND tier = ANY($${params.length})`;
  }
  params.push(limit);
  const sql = `
    SELECT puuid, tier
    FROM players
    WHERE ${where}
    ORDER BY last_ingest_at ASC NULLS FIRST, lp DESC NULLS LAST
    LIMIT $${params.length}
  `;
  try {
    const { rows } = await pool().query(sql, params);
    return rows.map((r) => ({ puuid: r.puuid as string, tier: (r.tier ?? null) as string | null }));
  } catch (e) {
    log.error("getIngestWorklist:", (e as Error).message);
    return [];
  }
}

/** Epoch SECONDS of the earliest match we've stored on `patchPrefix` (≈ when
 *  the patch dropped). Used to bound match-id pagination TIGHTLY to the current
 *  patch — otherwise a time-window fallback pulls in old-patch games we fetch
 *  just to discard (most of the wasted calls). Converges to the true patch
 *  start as we ingest more. Null if we have no matches on the patch yet. */
export async function getPatchStartEpoch(patchPrefix: string): Promise<number | null> {
  try {
    const { rows } = await pool().query(
      `SELECT EXTRACT(EPOCH FROM MIN(game_creation))::bigint AS e
         FROM matches WHERE game_version LIKE $1`,
      [`${patchPrefix}.%`]
    );
    const e = rows[0]?.e;
    return e != null ? Number(e) : null;
  } catch (err) {
    log.error("getPatchStartEpoch:", (err as Error).message);
    return null;
  }
}

/** Load puuid→tier for an ENTIRE platform's players into memory (small for
 *  apex). Used to stamp participants at ingest with zero per-match queries. */
export async function loadTierMap(platform: string): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  const PAGE = 1000;
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supa
      .from("players")
      .select("puuid, tier")
      .eq("platform", platform)
      .not("tier", "is", null)
      .range(from, from + PAGE - 1);
    if (error) {
      log.error("loadTierMap:", error.message);
      break;
    }
    for (const r of data ?? []) if (r.tier) out.set(r.puuid, r.tier);
    if (!data || data.length < PAGE) break;
  }
  return out;
}

// ── global match dedup ─────────────────────────────────────────────
/** Of the given match ids, which are already stored (so we skip re-fetch).
 *  Global (by match_id), so the same apex game shared by 10 tracked players
 *  is fetched once, not ten times. */
export async function existingMatchIds(matchIds: string[]): Promise<Set<string>> {
  const seen = new Set<string>();
  for (const c of chunk(matchIds, 300)) {
    const { data, error } = await supa.from("matches").select("match_id").in("match_id", c);
    if (error) {
      log.error("existingMatchIds:", error.message);
      continue;
    }
    for (const r of data ?? []) seen.add(r.match_id);
  }
  return seen;
}

// ── match upserts ──────────────────────────────────────────────────
export async function upsertMatchCore(row: Record<string, unknown>): Promise<void> {
  const { error } = await supa.from("matches").upsert(row, { onConflict: "match_id" });
  if (error) log.error("matches.upsert:", error.message);
}
export async function upsertMatchTeams(rows: Record<string, unknown>[]): Promise<void> {
  if (rows.length === 0) return;
  const { error } = await supa.from("match_teams").upsert(rows, { onConflict: "match_id,team_id" });
  if (error) log.error("match_teams.upsert:", error.message);
}
export async function upsertParticipants(rows: Record<string, unknown>[]): Promise<void> {
  if (rows.length === 0) return;
  const { error } = await supa
    .from("participants")
    .upsert(rows, { onConflict: "match_id,participant_id" });
  if (error) log.error("participants.upsert:", error.message);
}

// ── bulk inserts (pg multi-row) ────────────────────────────────────
// For mass ingest, PostgREST's one-row-per-request upserts are the bottleneck
// (millions of matches = tens of millions of round-trips). These build a
// single multi-row `INSERT ... ON CONFLICT DO NOTHING` per chunk via the pg
// pool. Rows are pre-deduped against `matches`, so conflicts are rare and
// DO NOTHING is the right semantics (a played match is immutable).
import { DB_MATCH_CHUNK, DB_PARTICIPANT_CHUNK } from "../config";

const MATCH_COLS = [
  "match_id", "platform", "game_creation", "game_duration_seconds",
  "game_version", "queue_id",
];
const TEAM_COLS = [
  "match_id", "team_id", "win", "first_dragon", "first_baron",
  "towers_destroyed", "dragons", "barons",
];
const PARTICIPANT_COLS = [
  "match_id", "participant_id", "puuid", "summoner_name", "riot_id_game_name",
  "riot_id_tagline", "team_id", "champion_id", "champion_name", "role", "lane",
  "win", "kills", "deaths", "assists", "gold_earned", "total_damage_to_champions",
  "vision_score", "total_minions_killed", "neutral_minions_killed", "total_cs",
  "champ_level", "time_played", "solo_kills", "summoner1_id", "summoner2_id",
  "item0", "item1", "item2", "item3", "item4", "item5", "item6",
  "perk_keystone", "perk_primary_style", "perk_sub_style", "kill_participation",
  "damage_share", "gold_at_10", "cs_at_10", "xp_at_10", "damage_at_10",
  "kills_at_10", "deaths_at_10", "assists_at_10", "tier",
  // compact build-order + precise runes (replaces participant_item_events)
  "legendary_order", "boots", "perk_primary", "perk_secondary", "stat_perks",
];

async function bulkInsert(
  table: string,
  cols: string[],
  rows: Record<string, unknown>[],
  chunkSize: number
): Promise<void> {
  if (rows.length === 0) return;
  const p = pool();
  for (const c of chunk(rows, chunkSize)) {
    const values: unknown[] = [];
    const tuples = c.map((row, r) => {
      const ph = cols.map((col, i) => {
        values.push(row[col] ?? null);
        return `$${r * cols.length + i + 1}`;
      });
      return `(${ph.join(",")})`;
    });
    const text =
      `INSERT INTO ${table} (${cols.join(",")}) VALUES ${tuples.join(",")} ` +
      `ON CONFLICT DO NOTHING`;
    try {
      await p.query(text, values);
    } catch (e) {
      log.error(`bulkInsert ${table}:`, (e as Error).message);
    }
  }
}

export const bulkInsertMatches = (rows: Record<string, unknown>[]) =>
  bulkInsert("matches", MATCH_COLS, rows, DB_MATCH_CHUNK);
export const bulkInsertMatchTeams = (rows: Record<string, unknown>[]) =>
  bulkInsert("match_teams", TEAM_COLS, rows, DB_MATCH_CHUNK);
export const bulkInsertParticipants = (rows: Record<string, unknown>[]) =>
  bulkInsert("participants", PARTICIPANT_COLS, rows, DB_PARTICIPANT_CHUNK);

// item-purchase events (build order) from the timeline — only present when
// INGEST_TIMELINE is on. Unique on (match_id, participant_id, ts_ms, event_type,
// item_id), so ON CONFLICT DO NOTHING dedups re-ingests.
const ITEM_EVENT_COLS = ["match_id", "participant_id", "puuid", "ts_ms", "event_type", "item_id", "gold"];
export const bulkInsertItemEvents = (rows: Record<string, unknown>[]) =>
  bulkInsert("participant_item_events", ITEM_EVENT_COLS, rows, DB_PARTICIPANT_CHUNK);
