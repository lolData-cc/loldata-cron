// src/jobs/snapshot-champion-stats.ts
// Daily cron: pre-compute champion stats for each champion/role/tier combo.
import { config } from "dotenv";
config({ override: true });

import pg from "pg";
import { log } from "../logger";

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  log.error("CHAMP_SNAP", "DATABASE_URL not set");
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
  idleTimeoutMillis: 30000,
});

const TIER_FILTERS = [null, "EMERALD", "EMERALD+", "DIAMOND", "DIAMOND+", "MASTER", "MASTER+", "GRANDMASTER", "CHALLENGER"];

async function main() {
  const startTime = Date.now();
  log.info("CHAMP_SNAP", "Starting champion stats snapshot...");

  const client = await pool.connect();
  await client.query("SET statement_timeout = '600s'");

  // Get all champion/role combos with enough games
  const { rows: combos } = await client.query(`
    SELECT DISTINCT champion_id, role
    FROM participants
    WHERE role IS NOT NULL AND role != '' AND role != 'Invalid'
      AND champion_id IS NOT NULL
    GROUP BY champion_id, role
    HAVING count(*) >= 50
    ORDER BY champion_id, role
  `);

  log.info("CHAMP_SNAP", `Found ${combos.length} champion/role combos`);

  const today = new Date().toISOString().slice(0, 10);

  // Delete today's old snapshots
  await client.query("DELETE FROM champion_stats_snapshots WHERE snapshot_date = $1", [today]);

  let success = 0;
  let failed = 0;

  // For the base (no tier filter), use the fast get_champion_stats RPC (materialized views)
  log.info("CHAMP_SNAP", "=== Base snapshots (all tiers, from materialized views) ===");
  for (let i = 0; i < combos.length; i++) {
    const { champion_id, role } = combos[i];
    try {
      const { rows } = await client.query(
        "SELECT get_champion_stats($1, $2, NULL, 420, NULL, NULL, NULL) AS data",
        [champion_id, role]
      );
      const data = rows[0]?.data;
      if (data) {
        await client.query(
          `INSERT INTO champion_stats_snapshots (champion_id, role, snapshot_date, tier, data)
           VALUES ($1, $2, $3, NULL, $4)
           ON CONFLICT DO NOTHING`,
          [champion_id, role, today, JSON.stringify(data)]
        );
        success++;
      }
    } catch {
      failed++;
    }
    if ((i + 1) % 100 === 0) log.info("CHAMP_SNAP", `Base: ${i + 1}/${combos.length}`);
  }
  log.info("CHAMP_SNAP", `Base done: ${success} ok, ${failed} failed`);

  // For tier-filtered snapshots, compute core stats only (fast query, no matchup joins)
  log.info("CHAMP_SNAP", "=== Tier-filtered snapshots (core stats only) ===");
  const tierFilters = TIER_FILTERS.filter(t => t !== null) as string[];
  let tierSuccess = 0;
  let tierFailed = 0;

  for (const tier of tierFilters) {
    const t0 = Date.now();
    // Get tier puuids once
    const { rows: tierPuuids } = await client.query(
      `SELECT array_agg(puuid) as puuids FROM users WHERE split_part(rank, ' ', 1) = ANY(tier_filter_ranks($1))`,
      [tier]
    );
    const puuids = tierPuuids[0]?.puuids;
    if (!puuids || puuids.length === 0) {
      log.info("CHAMP_SNAP", `Tier ${tier}: no players, skipping`);
      continue;
    }

    log.info("CHAMP_SNAP", `Tier ${tier}: ${puuids.length} players`);

    // Batch compute core stats for all champ/role combos in this tier
    const { rows: tierStats } = await client.query(`
      SELECT champion_id, role,
        count(*)::int AS games,
        CASE WHEN count(*) = 0 THEN 0
          ELSE ROUND(100.0 * SUM(win::int)::numeric / count(*), 2) END AS winrate,
        ROUND(AVG(kills)::numeric, 2) AS avg_kills,
        ROUND(AVG(deaths)::numeric, 2) AS avg_deaths,
        ROUND(AVG(assists)::numeric, 2) AS avg_assists,
        ROUND(AVG(gold_earned)::numeric, 0) AS avg_gold,
        ROUND(AVG(total_damage_to_champions)::numeric, 0) AS avg_dmg
      FROM participants
      WHERE puuid = ANY($1)
        AND role IS NOT NULL AND role != '' AND role != 'Invalid'
      GROUP BY champion_id, role
      HAVING count(*) >= 10
    `, [puuids]);

    log.info("CHAMP_SNAP", `Tier ${tier}: ${tierStats.length} combos computed in ${Date.now() - t0}ms`);

    // Get base snapshots to merge matchup data
    for (const stat of tierStats) {
      try {
        // Try to get matchup data from the base snapshot
        const { rows: baseSnap } = await client.query(
          `SELECT data FROM champion_stats_snapshots
           WHERE champion_id = $1 AND role = $2 AND snapshot_date = $3 AND tier IS NULL`,
          [stat.champion_id, stat.role, today]
        );
        const baseData = baseSnap[0]?.data;

        const data = {
          core: {
            winrate: Number(stat.winrate),
            pickrate: null,
            banrate: null,
            gamesAnalyzed: stat.games,
            avgKDA: { kills: Number(stat.avg_kills), deaths: Number(stat.avg_deaths), assists: Number(stat.avg_assists) },
            avgCS: null,
            avgGold: Number(stat.avg_gold),
            avgDamage: Number(stat.avg_dmg),
          },
          // Reuse matchup/synergy data from base snapshot (tier-specific matchups too expensive to compute live)
          bestMatchups: baseData?.bestMatchups ?? [],
          worstMatchups: baseData?.worstMatchups ?? [],
          bestSynergies: baseData?.bestSynergies ?? [],
          worstCounters: baseData?.worstCounters ?? [],
          objectiveWinrates: baseData?.objectiveWinrates ?? { firstDragon: null, firstBaron: null },
          gamePhaseWinrates: baseData?.gamePhaseWinrates ?? [],
          meta: { queueId: 420, role: stat.role, tier, lastUpdatedUtc: new Date().toISOString() },
        };

        await client.query(
          `INSERT INTO champion_stats_snapshots (champion_id, role, snapshot_date, tier, data)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT DO NOTHING`,
          [stat.champion_id, stat.role, today, tier, JSON.stringify(data)]
        );
        tierSuccess++;
      } catch {
        tierFailed++;
      }
    }
  }

  log.info("CHAMP_SNAP", `Tier snapshots: ${tierSuccess} ok, ${tierFailed} failed`);

  client.release();
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log.info("CHAMP_SNAP", `Total: ${success + tierSuccess} snapshots in ${elapsed}s`);

  await pool.end();
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    log.error("CHAMP_SNAP", `Fatal: ${err.message}`);
    process.exit(1);
  });
