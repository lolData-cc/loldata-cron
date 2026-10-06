export const SUPABASE_URL = process.env.SUPABASE_URL!;
export const SUPABASE_KEY = process.env.SUPABASE_KEY!;
export const RIOT_API_KEY = process.env.RIOT_API_KEY!;

export const SEASON_START_EPOCH = Number(process.env.SEASON_START_EPOCH ?? 0);
export const SEASON_END_EPOCH = process.env.SEASON_END_EPOCH
  ? Number(process.env.SEASON_END_EPOCH)
  : undefined;

// Rate limits - PER ROUTING HOST (see riot.ts): each platform host
// (euw1/eun1/na1/kr) and each regional host (europe/americas/asia) gets its
// own 500:10s / 30000:10min on a production key. Prod ran 350 / 21000.
export const RIOT_LIMIT_10S = Number(process.env.RIOT_LIMIT_10S ?? 1800);
export const RIOT_LIMIT_10MIN = Number(process.env.RIOT_LIMIT_10MIN ?? 100000);

// Concurrency for parallel player resolution
export const CONCURRENCY = Number(process.env.CRON_CONCURRENCY ?? 10);

// RECONSTRUCTED (2026-10-06) from the edits that lived only on the Hetzner
// box (deleted) - see loldata-infra/services/loldata-cron.md. Regions come
// from INGEST_REGIONS (prod ran "EUW,EUNE,NA,KR"); invalid entries are
// dropped, an empty list falls back to EUW.
export type Region = "EUW" | "EUNE" | "NA" | "KR";
const ALL_REGIONS: Region[] = ["EUW", "EUNE", "NA", "KR"];
export const REGIONS: Region[] = (process.env.INGEST_REGIONS ?? "EUW")
  .split(",").map((s) => s.trim().toUpperCase())
  .filter((r): r is Region => (ALL_REGIONS as string[]).includes(r));
if (REGIONS.length === 0) REGIONS.push("EUW");
export type ActiveRegion = Region;

export const QUEUE_SOLO = "RANKED_SOLO_5x5" as const;
export const QUEUE_FLEX = "RANKED_FLEX_SR" as const;

export const REGION_ROUTING: Record<
  Region,
  { account: string; match: string; platform: string }
> = {
  EUW: {
    account: "europe.api.riotgames.com",
    match: "europe.api.riotgames.com",
    platform: "euw1.api.riotgames.com",
  },
  EUNE: {
    account: "europe.api.riotgames.com",
    match: "europe.api.riotgames.com",
    platform: "eun1.api.riotgames.com",
  },
  NA: {
    account: "americas.api.riotgames.com",
    match: "americas.api.riotgames.com",
    platform: "na1.api.riotgames.com",
  },
  KR: {
    account: "asia.api.riotgames.com",
    match: "asia.api.riotgames.com",
    platform: "kr.api.riotgames.com",
  },
};

export const TIER_ORDER = [
  "IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM",
  "EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER",
];

export const DIVISION_ORDER: Record<string, number> = {
  IV: 1, III: 2, II: 3, I: 4,
};

// ── Sub-apex seeding (ingest-elo-seed) ─────────────────────────
// Tiers seeded into `users` so update-season-stats ingests their matches.
// Default "above plat" sample = Emerald + Diamond (apex Master+ comes from
// ingest-ladder). All env-tunable so we can resize without a redeploy.
export const SEED_TIERS = (process.env.SEED_TIERS ?? "EMERALD,DIAMOND")
  .split(",").map((s) => s.trim().toUpperCase()).filter(Boolean);
export const DIVISIONS = (process.env.SEED_DIVISIONS ?? "I,II,III,IV")
  .split(",").map((s) => s.trim().toUpperCase()).filter(Boolean);
// Players sampled per (tier × division × queue) bracket — league-v4 paginates by
// LP desc, so this takes the top-LP slice of each bracket (a large, representative
// sample; full coverage adds storage with ~zero statistical gain).
export const SEED_SAMPLE_PER_BRACKET = Number(process.env.SEED_SAMPLE_PER_BRACKET ?? 1500);

// ── Cloudflare R2 (S3-compatible) ──────────────────────────────
export const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID!;
export const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
export const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
export const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME ?? "loldata-cdn";

