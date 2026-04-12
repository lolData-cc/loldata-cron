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

export async function upsertUsers(rows: Record<string, unknown>[]): Promise<void> {
  const CHUNK = 100;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    let retries = 3;
    while (retries > 0) {
      const { error } = await supabase.from("users").upsert(chunk, { onConflict: "puuid" });
      if (!error) break;
      if (error.code === "40P01" && retries > 1) {
        // Deadlock — wait and retry
        const delay = (4 - retries) * 1000 + Math.random() * 1000;
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
      .select("puuid, region, rank")
      .not("puuid", "is", null)
      .not("name", "is", null)
      .eq("region", "EUW")
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
