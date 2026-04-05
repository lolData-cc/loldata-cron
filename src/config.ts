export const SUPABASE_URL = process.env.SUPABASE_URL!;
export const SUPABASE_KEY = process.env.SUPABASE_KEY!;
export const RIOT_API_KEY = process.env.RIOT_API_KEY!;

export const SEASON_START_EPOCH = Number(process.env.SEASON_START_EPOCH ?? 0);
export const SEASON_END_EPOCH = process.env.SEASON_END_EPOCH
  ? Number(process.env.SEASON_END_EPOCH)
  : undefined;

// Rate limits (with safety margin — production key: 2000/10s per endpoint)
export const RIOT_LIMIT_10S = Number(process.env.RIOT_LIMIT_10S ?? 1800);
export const RIOT_LIMIT_10MIN = Number(process.env.RIOT_LIMIT_10MIN ?? 100000);

// Concurrency for parallel player resolution (limited by Supabase connection pool)
export const CONCURRENCY = Number(process.env.CRON_CONCURRENCY ?? 20);

// Regions to actively ingest (EUW only for now)
export const REGIONS = ["EUW"] as const;
export type Region = "EUW" | "NA" | "KR"; // All supported regions for routing
export type ActiveRegion = (typeof REGIONS)[number]; // Regions we actively process

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

// ── Cloudflare R2 (S3-compatible) ──────────────────────────────
export const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID!;
export const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
export const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
export const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME ?? "loldata-cdn";

