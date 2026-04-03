import { RIOT_API_KEY, REGION_ROUTING, REGIONS, RIOT_LIMIT_10S, RIOT_LIMIT_10MIN } from "./config";
import type { Region } from "./config";
import type { QueueApi, LadderEntry, RankedEntry, LeagueEntryDTO } from "./types";
import { DualWindowRateLimiter } from "./rate-limiter";
import { log } from "./logger";

const limiter = new DualWindowRateLimiter(RIOT_LIMIT_10S, 10, RIOT_LIMIT_10MIN, 600);

// ── Decrypt error (wrong regional routing for puuid) ────────────────

export class DecryptError extends Error {
  constructor() { super("Exception decrypting"); }
}

// ── Generic fetch with rate limiting + retries ──────────────────────

async function riotFetch<T>(url: string, retries = 3): Promise<T | null> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    await limiter.acquire();
    try {
      const res = await fetch(url, {
        headers: { "X-Riot-Token": RIOT_API_KEY },
        signal: AbortSignal.timeout(15_000),
      });

      if (res.status === 429) {
        const retryAfter = Number(res.headers.get("Retry-After") ?? 2);
        log.warn("RIOT_429", `Rate limited, retry after ${retryAfter}s`, { url: url.slice(0, 120) });
        await Bun.sleep((retryAfter + 0.5) * 1000);
        continue;
      }
      if (res.status >= 500) {
        log.warn("RIOT_5XX", `Server error ${res.status}`, { url: url.slice(0, 120), attempt });
        await Bun.sleep(1500);
        continue;
      }
      if (res.status === 404) return null;
      if (!res.ok) {
        const body = await res.text().catch(() => "");
        // Riot returns 400 "Exception decrypting" when puuid is queried against wrong region
        if (res.status === 400 && body.includes("Exception decrypting")) {
          throw new DecryptError();
        }
        log.error("RIOT_ERR", `${res.status}`, { url: url.slice(0, 300), body: body.slice(0, 200) });
        return null;
      }
      return (await res.json()) as T;
    } catch (err: unknown) {
      if (err instanceof DecryptError) throw err; // Don't retry, bubble up
      const msg = err instanceof Error ? err.message : String(err);
      log.warn("RIOT_NET", `Network error: ${msg}`, { url: url.slice(0, 120), attempt });
      await Bun.sleep(1500);
    }
  }
  return null;
}

// ── Region cache for puuids (avoids redundant fallback calls) ───────
const _puuidRegionCache = new Map<string, Region>();

export function getCachedRegion(puuid: string): Region | undefined {
  return _puuidRegionCache.get(puuid);
}

// ── Ladder endpoints ────────────────────────────────────────────────

type LeagueResponse = { entries: LadderEntry[] };

export async function getChallenger(queue: QueueApi, region: Region): Promise<LadderEntry[]> {
  const host = REGION_ROUTING[region].platform;
  const data = await riotFetch<LeagueResponse>(
    `https://${host}/lol/league/v4/challengerleagues/by-queue/${queue}`,
  );
  return (data?.entries ?? []).map((e) => ({ ...e, tier: "CHALLENGER" as const }));
}

export async function getGrandmaster(queue: QueueApi, region: Region): Promise<LadderEntry[]> {
  const host = REGION_ROUTING[region].platform;
  const data = await riotFetch<LeagueResponse>(
    `https://${host}/lol/league/v4/grandmasterleagues/by-queue/${queue}`,
  );
  return (data?.entries ?? []).map((e) => ({ ...e, tier: "GRANDMASTER" as const }));
}

export async function getMaster(queue: QueueApi, region: Region): Promise<LadderEntry[]> {
  const host = REGION_ROUTING[region].platform;
  const data = await riotFetch<LeagueResponse>(
    `https://${host}/lol/league/v4/masterleagues/by-queue/${queue}`,
  );
  return (data?.entries ?? []).map((e) => ({ ...e, tier: "MASTER" as const }));
}

// ── Sub-Master league entries (Diamond, Emerald, Platinum, Gold, …) ─

export async function getLeagueEntries(
  queue: QueueApi,
  tier: string,
  division: string,
  page: number,
  region: Region,
): Promise<LeagueEntryDTO[]> {
  const host = REGION_ROUTING[region].platform;
  return (
    (await riotFetch<LeagueEntryDTO[]>(
      `https://${host}/lol/league/v4/entries/${queue}/${tier}/${division}?page=${page}`,
    )) ?? []
  );
}

// ── Summoner / Account resolution ───────────────────────────────────

type SummonerData = { puuid: string; profileIconId: number };
type AccountData = { puuid: string; gameName: string; tagLine: string };

export async function getSummonerByEncryptedId(
  encryptedId: string,
  region: Region,
): Promise<SummonerData | null> {
  const host = REGION_ROUTING[region].platform;
  return riotFetch<SummonerData>(`https://${host}/lol/summoner/v4/summoners/${encryptedId}`);
}

export async function getSummonerByPuuid(
  puuid: string,
  region: Region,
): Promise<SummonerData | null> {
  const host = REGION_ROUTING[region].platform;
  return riotFetch<SummonerData>(
    `https://${host}/lol/summoner/v4/summoners/by-puuid/${encodeURIComponent(puuid)}`,
  );
}

export async function getAccountByPuuid(
  puuid: string,
  region: Region,
): Promise<AccountData | null> {
  const host = REGION_ROUTING[region].account;
  return riotFetch<AccountData>(
    `https://${host}/riot/account/v1/accounts/by-puuid/${encodeURIComponent(puuid)}`,
  );
}

// ── Ranked entries ──────────────────────────────────────────────────

export async function getRankedEntries(
  puuid: string,
  region: Region,
): Promise<RankedEntry[]> {
  const host = REGION_ROUTING[region].platform;
  return (
    (await riotFetch<RankedEntry[]>(
      `https://${host}/lol/league/v4/entries/by-puuid/${encodeURIComponent(puuid)}`,
    )) ?? []
  );
}

// ── Match endpoints (for season stats) ──────────────────────────────

function buildMatchIdsUrl(puuid: string, host: string, opts: {
  start?: number;
  count?: number;
  queue?: number;
  type?: string;
  startTime?: number;
  endTime?: number;
}): string {
  // Plain string URL (no new URL() — avoids potential Bun encoding issues)
  const base = `https://${host}/lol/match/v5/matches/by-puuid/${puuid.trim()}/ids`;
  const params: string[] = [];
  if (opts.start != null) params.push(`start=${opts.start}`);
  if (opts.count != null) params.push(`count=${opts.count}`);
  if (opts.queue != null) params.push(`queue=${opts.queue}`);
  if (opts.type) params.push(`type=${opts.type}`);
  if (opts.startTime != null) params.push(`startTime=${opts.startTime}`);
  if (opts.endTime != null) params.push(`endTime=${opts.endTime}`);
  return params.length ? `${base}?${params.join("&")}` : base;
}

export async function getMatchIds(
  puuid: string,
  region: Region,
  opts: {
    start?: number;
    count?: number;
    queue?: number;
    type?: string;
    startTime?: number;
    endTime?: number;
  },
): Promise<string[]> {
  // Build list of regions to try: cached first, then stored, then remaining
  const cached = _puuidRegionCache.get(puuid);
  const regionsToTry: Region[] = [];
  if (cached) regionsToTry.push(cached);
  if (!regionsToTry.includes(region)) regionsToTry.push(region);
  for (const r of REGIONS) {
    if (!regionsToTry.includes(r)) regionsToTry.push(r);
  }

  for (const r of regionsToTry) {
    const host = REGION_ROUTING[r].match;
    const fullUrl = buildMatchIdsUrl(puuid, host, opts);
    // Log the first ever URL for debugging
    if (_puuidRegionCache.size === 0) {
      log.info("MATCH_DEBUG", `First getMatchIds URL: ${fullUrl}`);
    }
    try {
      const result = await riotFetch<string[]>(fullUrl);
      // Success! Cache this region for future calls
      _puuidRegionCache.set(puuid, r);
      return result ?? [];
    } catch (e) {
      if (e instanceof DecryptError) {
        // Decrypt error means the puuid itself is invalid/stale — no point
        // trying other regions (saves 2 wasted API calls per bad puuid).
        _badPuuids.add(puuid);
        return [];
      }
      throw e; // Re-throw other errors
    }
  }
  return [];
}

export async function getMatchDetails(matchId: string, region: Region): Promise<any | null> {
  // Match IDs are prefixed with platform (e.g. EUW1_12345), derive region from that
  const host = REGION_ROUTING[region].match;
  return riotFetch(`https://${host}/lol/match/v5/matches/${matchId}`);
}

export async function getMatchTimeline(matchId: string, region: Region): Promise<any | null> {
  const host = REGION_ROUTING[region].match;
  return riotFetch(`https://${host}/lol/match/v5/matches/${matchId}/timeline`);
}

// ── Bad puuid tracking ──────────────────────────────────────────────
const _badPuuids = new Set<string>();
export function isBadPuuid(puuid: string): boolean { return _badPuuids.has(puuid); }
export function markBadPuuid(puuid: string): void { _badPuuids.add(puuid); }
