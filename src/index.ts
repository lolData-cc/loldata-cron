import { log } from "./logger";
import { RIOT_API_KEY, REGION_ROUTING, SUPABASE_URL, SUPABASE_KEY } from "./config";
import { createClient } from "@supabase/supabase-js";
import { runIngestLadder } from "./jobs/ingest-ladder";

import { runTrackRanks } from "./jobs/track-ranks";
import { runUpdateSeasonStats } from "./jobs/update-stats";

/** Quick Riot API health check — catches expired dev keys before wasting time */
async function checkApiKey(): Promise<boolean> {
  try {
    // 1) Check key validity with the status endpoint
    const res = await fetch(
      `https://${REGION_ROUTING.EUW.platform}/lol/status/v4/platform-data`,
      { headers: { "X-Riot-Token": RIOT_API_KEY }, signal: AbortSignal.timeout(10_000) },
    );
    if (res.status === 401 || res.status === 403) {
      log.error("API_KEY", `Riot API key is invalid or expired (${res.status}). Regenerate at developer.riotgames.com`);
      return false;
    }
    log.info("API_KEY", "Riot API key is valid ✓");

    // 2) Test Match V5 endpoint with a real puuid from the DB
    const db = createClient(SUPABASE_URL, SUPABASE_KEY);
    const { data } = await db.from("users").select("puuid").not("puuid", "is", null).limit(1).single();
    if (data?.puuid) {
      const puuid = data.puuid.trim();
      const matchUrl = `https://${REGION_ROUTING.EUW.match}/lol/match/v5/matches/by-puuid/${puuid}/ids?count=1`;
      log.info("API_TEST", `Testing Match V5 with puuid=${puuid.slice(0, 20)}...`);
      log.info("API_TEST", `Full URL: ${matchUrl}`);
      const matchRes = await fetch(matchUrl, {
        headers: { "X-Riot-Token": RIOT_API_KEY },
        signal: AbortSignal.timeout(10_000),
      });
      const matchBody = await matchRes.text();
      log.info("API_TEST", `Match V5 response: ${matchRes.status} — ${matchBody.slice(0, 300)}`);
      if (matchRes.status === 400 && matchBody.includes("Exception decrypting")) {
        // Try a different approach — use the api_key query param like Python does
        const matchUrl2 = `https://${REGION_ROUTING.EUW.match}/lol/match/v5/matches/by-puuid/${puuid}/ids?count=1&api_key=${RIOT_API_KEY}`;
        log.info("API_TEST", "Retrying with api_key as query param...");
        const matchRes2 = await fetch(matchUrl2, { signal: AbortSignal.timeout(10_000) });
        const matchBody2 = await matchRes2.text();
        log.info("API_TEST", `Match V5 (query param): ${matchRes2.status} — ${matchBody2.slice(0, 300)}`);
      }
    }

    return true;
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    log.warn("API_KEY", `Health check error: ${msg}, proceeding anyway`);
    return true;
  }
}

async function main() {
  const startTime = Date.now();
  log.info("CRON_START", "Nightly cron job started", {
    timestamp: new Date().toISOString(),
  });

  // Pre-flight: verify API key before processing 39K+ users
  const keyOk = await checkApiKey();
  if (!keyOk) {
    log.error("CRON_ABORT", "Aborting — fix RIOT_API_KEY in .env and re-run");
    process.exit(1);
  }

  // Step 1: Ingest apex ladder players (Challenger/GM/Master)
  try {
    await runIngestLadder();
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    log.error("CRON_FATAL", `ingest-ladder failed: ${msg}`);
  }

  // Step 2: Track ranks (fast — 1 API call per user)
  try {
    await runTrackRanks();
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    log.error("CRON_FATAL", `track-ranks failed: ${msg}`);
  }

  // Step 3: Update season stats (heaviest — many API calls per user, now with participant fan-out)
  try {
    await runUpdateSeasonStats();
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    log.error("CRON_FATAL", `update-season-stats failed: ${msg}`);
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  log.info("CRON_END", `Nightly cron completed in ${totalElapsed}min`);

  process.exit(0);
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  log.error("CRON_CRASH", `Unhandled error: ${msg}`);
  process.exit(1);
});
