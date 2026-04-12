import { CONCURRENCY } from "../config";
import type { Region } from "../config";
import { getRankedEntries } from "../riot";
import { supabase, getMasterPlusUserPuuids, getUserPeakData, rankToScore, formatRank } from "../db";
import { log } from "../logger";

async function updatePlayerRank(puuid: string, region: Region): Promise<void> {
  const entries = await getRankedEntries(puuid, region);
  if (!entries || entries.length === 0) return;

  const solo = entries.find((e) => e.queueType === "RANKED_SOLO_5x5");
  const flex = entries.find((e) => e.queueType === "RANKED_FLEX_SR");

  const peak = await getUserPeakData(puuid);
  const update: Record<string, unknown> = {};

  // ── Solo queue ────────────────────────────────────────────────
  if (solo) {
    const currentRank = formatRank(solo.tier, solo.rank);
    const currentScore = rankToScore(solo.tier, solo.rank, solo.leaguePoints);

    update.rank = currentRank;

    let storedPeakScore = 0;
    if (peak?.peak_rank && peak.peak_rank !== "Unranked") {
      const [savedTier, savedDivision] = peak.peak_rank.split(" ");
      storedPeakScore = rankToScore(savedTier!, savedDivision, peak.peak_lp ?? 0);
    }

    if (currentScore > storedPeakScore) {
      update.peak_rank = currentRank;
      update.peak_lp = solo.leaguePoints;
    }
  }

  // ── Flex queue ────────────────────────────────────────────────
  if (flex) {
    const currentFlexRank = formatRank(flex.tier, flex.rank);
    const currentFlexScore = rankToScore(flex.tier, flex.rank, flex.leaguePoints);

    update.flex_rank = currentFlexRank;
    update.flex_lp = flex.leaguePoints;

    let storedPeakFlexScore = 0;
    if (peak?.peak_flex_rank && peak.peak_flex_rank !== "Unranked") {
      const [savedTier, savedDivision] = peak.peak_flex_rank.split(" ");
      storedPeakFlexScore = rankToScore(savedTier!, savedDivision, peak.peak_flex_lp ?? 0);
    }

    if (currentFlexScore > storedPeakFlexScore) {
      update.peak_flex_rank = currentFlexRank;
      update.peak_flex_lp = flex.leaguePoints;
    }
  }

  if (Object.keys(update).length === 0) return;

  const { error } = await supabase.from("users").update(update).eq("puuid", puuid);
  if (error) {
    log.error("RANK_UPDATE", `Failed for ${puuid.slice(0, 12)}...`, { message: error.message });
  }
}

export async function runTrackRanks(): Promise<void> {
  const startTime = Date.now();
  log.info("JOB_START", "track-ranks started");

  const users = await getMasterPlusUserPuuids();
  log.info("RANK", `Tracking ranks for ${users.length} Master+ users`);

  let updated = 0;
  let errors = 0;

  for (let i = 0; i < users.length; i += CONCURRENCY) {
    const batch = users.slice(i, i + CONCURRENCY);

    const results = await Promise.allSettled(
      batch.map(({ puuid, region }) => updatePlayerRank(puuid, region as Region)),
    );

    for (const r of results) {
      if (r.status === "fulfilled") updated++;
      else {
        errors++;
        log.warn("RANK_ERR", `Player rank update failed: ${r.reason?.message}`);
      }
    }

    if (i > 0 && i % 500 === 0) {
      const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      log.info("RANK_PROGRESS", `${i}/${users.length} (${elapsed}min)`);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  log.info("JOB_END", `track-ranks completed in ${elapsed}min`, { updated, errors });
}

runTrackRanks().catch(console.error);
