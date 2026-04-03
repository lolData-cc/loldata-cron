import { log } from "./logger";
import { RIOT_API_KEY, REGION_ROUTING } from "./config";
import { runUpdateSeasonStats } from "./jobs/update-stats";

async function main() {
  const allUsers = process.argv.includes("--all");
  const masterOnly = !allUsers; // Default: Master+ only (safe)
  log.info("MATCHES_ONLY", `Starting match ingestion${masterOnly ? " (Master+ only)" : " (ALL users — use with caution)"}`);

  // Quick API key check
  const res = await fetch(
    `https://${REGION_ROUTING.EUW.platform}/lol/status/v4/platform-data`,
    { headers: { "X-Riot-Token": RIOT_API_KEY }, signal: AbortSignal.timeout(10_000) },
  );
  if (res.status === 401 || res.status === 403) {
    log.error("API_KEY", `Riot API key is invalid (${res.status})`);
    process.exit(1);
  }
  log.info("API_KEY", "Riot API key valid ✓");

  const start = Date.now();
  await runUpdateSeasonStats({ masterPlusOnly: masterOnly });
  const elapsed = ((Date.now() - start) / 1000 / 60).toFixed(1);
  log.info("DONE", `Match ingestion completed in ${elapsed}min`);

  process.exit(0);
}

main().catch((err) => {
  log.error("CRASH", err instanceof Error ? err.message : String(err));
  process.exit(1);
});
