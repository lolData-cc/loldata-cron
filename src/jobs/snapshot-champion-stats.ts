// src/jobs/snapshot-champion-stats.ts
// Daily cron: pre-compute champion stats for each champion/role combo
// and store as JSONB snapshots for instant frontend loading.

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
  max: 3,
  idleTimeoutMillis: 30000,
});

async function main() {
  const startTime = Date.now();
  log.info("CHAMP_SNAP", "Starting champion stats snapshot...");

  await pool.query("SET statement_timeout = '300s'");

  // Get all champion/role combos with enough games
  const { rows: combos } = await pool.query(`
    SELECT champion_id, champion_name, role, count(*) as games
    FROM participants
    WHERE role IS NOT NULL AND role != '' AND role != 'Invalid'
    GROUP BY champion_id, champion_name, role
    HAVING count(*) >= 100
    ORDER BY games DESC
  `);

  log.info("CHAMP_SNAP", `Found ${combos.length} champion/role combos to snapshot`);

  let success = 0;
  let failed = 0;
  const today = new Date().toISOString().slice(0, 10);

  // Delete today's old snapshots
  await pool.query("DELETE FROM champion_stats_snapshots WHERE snapshot_date = $1", [today]);

  // Process in batches of 10 for controlled concurrency
  const BATCH_SIZE = 10;
  for (let i = 0; i < combos.length; i += BATCH_SIZE) {
    const batch = combos.slice(i, i + BATCH_SIZE);

    const results = await Promise.allSettled(
      batch.map(async (combo) => {
        const { champion_id, role } = combo;
        try {
          const { rows } = await pool.query(
            "SELECT get_champion_stats($1, NULL, NULL, 420, $2, NULL, NULL) AS data",
            [champion_id, role]
          );
          const data = rows[0]?.data;
          if (!data) return;

          await pool.query(
            `INSERT INTO champion_stats_snapshots (champion_id, role, snapshot_date, data)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (champion_id, role, snapshot_date) DO UPDATE SET data = $4, created_at = NOW()`,
            [champion_id, role, today, JSON.stringify(data)]
          );
          success++;
        } catch (err: any) {
          failed++;
          log.error("CHAMP_SNAP", `Failed ${combo.champion_name} ${role}: ${err.message}`);
        }
      })
    );

    if ((i + BATCH_SIZE) % 50 === 0 || i + BATCH_SIZE >= combos.length) {
      log.info("CHAMP_SNAP", `Progress: ${Math.min(i + BATCH_SIZE, combos.length)}/${combos.length} (${success} ok, ${failed} failed)`);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log.info("CHAMP_SNAP", `Done in ${elapsed}s — ${success} snapshots, ${failed} failed`);

  await pool.end();
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    log.error("CHAMP_SNAP", `Fatal: ${err.message}`);
    process.exit(1);
  });
