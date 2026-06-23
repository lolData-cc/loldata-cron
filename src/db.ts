import { createClient } from "@supabase/supabase-js";
import { SUPABASE_URL, SUPABASE_KEY, TIER_ORDER, DIVISION_ORDER } from "./config";
import { log } from "./logger";

export const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Port of rankToScore from getSummoner.ts:18-33
export function rankToScore(tier: string, division: string | undefined, lp: number): number {
  const base = TIER_ORDER.indexOf(tier.toUpperCase()) * 1000;
  const divisionScore = division ? (DIVISION_ORDER[division.toUpperCase()] ?? 0) : 0;
  return base + divisionScore * 100 + lp;
}

export function formatRank(tier: string, division?: string): string {
  if (!tier || tier === "Unranked") return "Unranked";
  if (["MASTER", "GRANDMASTER", "CHALLENGER"].includes(tier.toUpperCase())) {
    return tier.toUpperCase();
  }
  return `${tier.toUpperCase()} ${division?.toUpperCase() ?? "IV"}`;
}

// Serialize all user upserts to prevent deadlocks
let _upsertQueue: Promise<void> = Promise.resolve();

export function upsertUsers(rows: Record<string, unknown>[]): Promise<void> {
  const task = _upsertQueue.then(() => _doUpsertUsers(rows));
  _upsertQueue = task.catch(() => {});
  return task;
}

async function _doUpsertUsers(rows: Record<string, unknown>[]): Promise<void> {
  const CHUNK = 50;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    let retries = 3;
    while (retries > 0) {
      const { error } = await supabase.from("users").upsert(chunk, { onConflict: "puuid" });
      if (!error) break;
      if (error.code === "40P01" && retries > 1) {
        const delay = (4 - retries) * 2000 + Math.random() * 2000;
        log.warn("DB_UPSERT", `Deadlock at chunk ${i}, retrying in ${Math.round(delay)}ms...`);
        await new Promise(r => setTimeout(r, delay));
        retries--;
      } else {
        log.error("DB_UPSERT", `users upsert failed at chunk ${i}`, {
          code: error.code,
          message: error.message,
        });
        break;
      }
    }
  }
}

export async function getAllUserPuuids(): Promise<{ puuid: string; region: string }[]> {
  const PAGE = 1000;
  const results: { puuid: string; region: string }[] = [];
  let offset = 0;
  let totalSkipped = 0;

  while (true) {
    const { data, error } = await supabase
      .from("users")
      .select("puuid, region")
      .not("puuid", "is", null)
      .not("name", "is", null)
      .not("region", "is", null)
      .range(offset, offset + PAGE - 1);

    if (error) {
      log.error("DB_QUERY", "Failed to fetch user puuids", { message: error.message });
      break;
    }
    if (!data || data.length === 0) break;

    // Real Riot puuids are ~78 chars (base64). Skip corrupted/short entries
    // that would waste 3 API calls each trying all regions before failing.
    const valid = data.filter((r) => r.puuid.length >= 40);
    totalSkipped += data.length - valid.length;
    results.push(...valid);
    if (data.length < PAGE) break;
    offset += PAGE;
  }

  if (totalSkipped > 0) {
    log.warn("DB_FILTER", `Filtered out ${totalSkipped} users with invalid short puuids (< 40 chars)`);
  }

  return results;
}

export async function getMasterPlusUserPuuids(): Promise<{ puuid: string; region: string }[]> {
  const PAGE = 1000;
  const results: { puuid: string; region: string }[] = [];
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from("users")
      .select("puuid, region, rank, lp")
      .not("puuid", "is", null)
      .not("name", "is", null)
      .eq("region", "EUW")
      .order("lp", { ascending: false, nullsFirst: false })
      .range(offset, offset + PAGE - 1);

    if (error) {
      log.error("DB_QUERY", "Failed to fetch master+ puuids", { message: error.message });
      break;
    }
    if (!data || data.length === 0) break;

    const APEX_TIERS = ["MASTER", "GRANDMASTER", "CHALLENGER"];
    const valid = data.filter((r) => {
      if (r.puuid.length < 40) return false;
      const tier = (r.rank ?? "").split(" ")[0]?.toUpperCase();
      return APEX_TIERS.includes(tier);
    });

    results.push(...valid.map((r) => ({ puuid: r.puuid, region: r.region })));
    if (data.length < PAGE) break;
    offset += PAGE;
  }

  log.info("DB_FILTER", `Found ${results.length} Master+ users`);
  return results;
}

// Users to PROCESS for match ingestion (update-season-stats). Broader than the
// apex-only rank-tracking set above: defaults to Emerald+ (env PROCESS_TIERS).
// NB the `.order("lp")` is non-unique → add a `puuid` tiebreaker so .range()
// pagination doesn't silently skip rows.
export async function getProcessableUserPuuids(): Promise<{ puuid: string; region: string }[]> {
  const PROCESS_TIERS = (process.env.PROCESS_TIERS ?? "EMERALD,DIAMOND,MASTER,GRANDMASTER,CHALLENGER")
    .split(",").map((s) => s.trim().toUpperCase()).filter(Boolean);
  // Server-side rank filter — the users table holds ~1.3M match-discovered rows
  // (mostly rank=NULL); without this we'd paginate ALL of them and filter in JS
  // (~1300 pages → effectively hangs). `rank` looks like "EMERALD II", so match
  // by tier prefix. Reduces the scan to the ~tens-of-k seeded/apex users.
  const orFilter = PROCESS_TIERS.map((t) => `rank.like.${t}*`).join(",");
  const PAGE = 1000;
  const results: { puuid: string; region: string }[] = [];
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from("users")
      .select("puuid, region, rank, lp")
      .not("puuid", "is", null)
      .not("name", "is", null)
      .eq("region", "EUW")
      .or(orFilter)
      .order("lp", { ascending: false, nullsFirst: false })
      .range(offset, offset + PAGE - 1);

    if (error) {
      log.error("DB_QUERY", "Failed to fetch processable puuids", { message: error.message });
      break;
    }
    if (!data || data.length === 0) break;

    const valid = data.filter((r) => {
      if (r.puuid.length < 40) return false;
      const tier = (r.rank ?? "").split(" ")[0]?.toUpperCase();
      return PROCESS_TIERS.includes(tier);
    });

    results.push(...valid.map((r) => ({ puuid: r.puuid, region: r.region })));
    if (data.length < PAGE) break;
    offset += PAGE;
  }

  // ── Discovery backlog ───────────────────────────────────────────────
  // The ~14.7k Emerald+ seeds above are basically fully ingested (≈1.5M matches
  // ≈ all their ranked games), so a pass over them yields almost no new matches.
  // The growth past that lives in the ~1.08M match-discovered EUW users with
  // rank=NULL that the rank filter skips. Pull a ROTATING, capped batch of them
  // via claim_discovery_batch() — an atomic select+mark (by last_searched_at) so
  // each pass takes the next N. Env-gated + reversible: DISCOVERY_BATCH=0
  // disables it; pace the DB-write/IO load by tuning the batch size.
  // PostgREST caps every response (incl. RPC results) at ~1000 rows, so claim in
  // 1000-row chunks until DISCOVERY_BATCH is reached or the backlog runs dry —
  // each chunk marks exactly what it returns (no marked-but-skipped waste), and
  // DISCOVERY_BATCH can scale arbitrarily (tune it to pace DB-write/IO load).
  const DISCOVERY_BATCH = Number(process.env.DISCOVERY_BATCH ?? 0);
  if (DISCOVERY_BATCH > 0) {
    const CHUNK = 1000;
    const have = new Set(results.map((r) => r.puuid));
    let added = 0;
    try {
      for (let got = 0; got < DISCOVERY_BATCH; got += CHUNK) {
        const n = Math.min(CHUNK, DISCOVERY_BATCH - got);
        const { data: disc, error: dErr } = await supabase.rpc("claim_discovery_batch", { n });
        if (dErr) { log.warn("DB_FILTER", `discovery batch failed: ${dErr.message}`); break; }
        if (!Array.isArray(disc) || disc.length === 0) break;
        for (const r of disc as Array<{ puuid: string; region: string }>) {
          if (r?.puuid && r.puuid.length >= 40 && !have.has(r.puuid)) {
            have.add(r.puuid);
            results.push({ puuid: r.puuid, region: r.region || "EUW" });
            added++;
          }
        }
        if (disc.length < n) break; // backlog exhausted
      }
      if (added > 0) log.info("DB_FILTER", `+ ${added} discovery-backlog users (rank=NULL, rotated, target=${DISCOVERY_BATCH})`);
    } catch (e: any) {
      log.warn("DB_FILTER", `discovery batch threw: ${e?.message ?? e}`);
    }
  }

  log.info("DB_FILTER", `Found ${results.length} processable users (${PROCESS_TIERS.join("/")})`);
  return results;
}

export async function getUserPeakData(puuid: string): Promise<{
  peak_rank: string | null;
  peak_lp: number | null;
  peak_flex_rank: string | null;
  peak_flex_lp: number | null;
} | null> {
  const { data, error } = await supabase
    .from("users")
    .select("peak_rank, peak_lp, peak_flex_rank, peak_flex_lp")
    .eq("puuid", puuid)
    .maybeSingle();

  if (error) {
    log.error("DB_QUERY", "Failed to read peak data", { puuid: puuid.slice(0, 12), message: error.message });
    return null;
  }
  return data;
}
