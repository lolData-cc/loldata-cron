// src/jobs/snapshot-champion-stats.ts
// Daily cron: pre-compute champion stats + items for each champion/role/tier combo.
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
  max: 12,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  statement_timeout: 600000,
} as any);

const TIER_FILTERS = [null, "EMERALD", "EMERALD+", "DIAMOND", "DIAMOND+", "MASTER", "MASTER+", "GRANDMASTER", "CHALLENGER"];

async function main() {
  const startTime = Date.now();
  log.info("CHAMP_SNAP", "Starting champion stats snapshot...");

  const client = await pool.connect();
  await client.query("SET statement_timeout = '600s'");

  const today = new Date().toISOString().slice(0, 10);

  // Delete today's old snapshots
  await client.query("DELETE FROM champion_stats_snapshots WHERE snapshot_date = $1", [today]);

  // ── Step 0: Refresh materialized views ──
  const ALL_VIEWS = [
    "mv_champion_role_stats",
    "mv_lane_opponents",
    "mv_lane_matchups",
    "mv_synergies",
    "mv_game_phases",
    "mv_objective_winrates",
    "participant_legendary_purchases",
  ];

  for (const mv of ALL_VIEWS) {
    try {
      log.info("CHAMP_SNAP", `Refreshing ${mv}...`);
      const t = Date.now();
      await client.query(`SET statement_timeout = '300s'`);
      await client.query(`REFRESH MATERIALIZED VIEW ${mv}`);
      log.info("CHAMP_SNAP", `${mv} refreshed in ${((Date.now() - t) / 1000).toFixed(1)}s`);
    } catch (e: any) {
      log.warn("CHAMP_SNAP", `Failed to refresh ${mv}: ${e.message?.slice(0, 100)}`);
    }
  }
  await client.query(`SET statement_timeout = '600s'`);

  // ── Step 1: Pre-compute ALL item data in one bulk query ──
  log.info("CHAMP_SNAP", "Pre-computing item data for all champions...");
  const t0 = Date.now();
  const { rows: allItems } = await client.query(`
    WITH all_items AS (
      SELECT p.champion_id, p.role,
        unnest(ARRAY[p.item0, p.item1, p.item2, p.item3, p.item4, p.item5]) AS iid,
        p.win
      FROM participants p
      WHERE p.role IS NOT NULL AND p.role != '' AND p.role != 'Invalid'
    ),
    counts AS (
      SELECT p.champion_id, p.role, count(*) AS total
      FROM participants p
      WHERE p.role IS NOT NULL AND p.role != '' AND p.role != 'Invalid'
      GROUP BY p.champion_id, p.role
    ),
    legendary AS (
      SELECT ai.champion_id, ai.role, ai.iid AS item_id, ai.win
      FROM all_items ai
      JOIN items i ON i.item_id = ai.iid
      WHERE i.is_legendary = true
    )
    SELECT l.champion_id, l.role, l.item_id,
      count(*)::int AS games,
      SUM(l.win::int)::int AS wins,
      ROUND(100.0 * SUM(l.win::int)::numeric / count(*), 2) AS winrate,
      ROUND(100.0 * count(*)::numeric / NULLIF(c.total, 0), 2) AS pick_rate
    FROM legendary l
    JOIN counts c ON c.champion_id = l.champion_id AND c.role = l.role
    GROUP BY l.champion_id, l.role, l.item_id, c.total
    HAVING count(*) >= 5
    ORDER BY l.champion_id, l.role, count(*) DESC
  `);
  log.info("CHAMP_SNAP", `Item data: ${allItems.length} rows in ${Date.now() - t0}ms`);

  // Build item lookup: key = "champId:role" → items array
  const itemMap = new Map<string, any[]>();
  for (const row of allItems) {
    const key = `${row.champion_id}:${row.role}`;
    if (!itemMap.has(key)) itemMap.set(key, []);
    const arr = itemMap.get(key)!;
    if (arr.length < 12) { // limit 12 per combo
      arr.push({
        item_id: row.item_id,
        games: row.games,
        wins: row.wins,
        winrate: Number(row.winrate),
        pick_rate: Number(row.pick_rate),
      });
    }
  }
  log.info("CHAMP_SNAP", `Item map: ${itemMap.size} champion/role combos`);

  // ── Step 2: Get all champion/role combos ──
  const { rows: combos } = await client.query(`
    SELECT DISTINCT champion_id, role
    FROM participants
    WHERE role IS NOT NULL AND role != '' AND role != 'Invalid'
    GROUP BY champion_id, role
    HAVING count(*) >= 50
    ORDER BY champion_id, role
  `);
  log.info("CHAMP_SNAP", `Found ${combos.length} champion/role combos`);

  // ── Step 3: Base snapshots (from materialized views + pre-computed items) ──
  log.info("CHAMP_SNAP", "=== Base snapshots ===");
  let success = 0;
  let failed = 0;
  const PARALLEL = 10;

  for (let i = 0; i < combos.length; i += PARALLEL) {
    const batch = combos.slice(i, i + PARALLEL);
    const results = await Promise.allSettled(batch.map(async ({ champion_id, role }: any) => {
      const [statsRes, runesRes] = await Promise.all([
        pool.query("SELECT get_champion_stats($1, $2, NULL, 420, NULL, NULL, NULL) AS data", [champion_id, role]),
        pool.query("SELECT * FROM champion_rune_stats($1, $2, NULL, 8)", [champion_id, role]),
      ]);
      const data = statsRes.rows[0]?.data;
      if (data) {
        data.items = itemMap.get(`${champion_id}:${role}`) ?? [];
        data.runes = runesRes.rows.map((r: any) => ({
          perk_keystone: r.perk_keystone,
          perk_primary_style: r.perk_primary_style,
          perk_sub_style: r.perk_sub_style,
          games: Number(r.games),
          wins: Number(r.wins),
          winrate: Number(r.winrate),
          pick_rate: Number(r.pick_rate),
        }));
        await pool.query(
          `INSERT INTO champion_stats_snapshots (champion_id, role, snapshot_date, tier, data)
           VALUES ($1, $2, $3, NULL, $4)
           ON CONFLICT (champion_id, role, snapshot_date, tier) DO UPDATE SET data = EXCLUDED.data`,
          [champion_id, role, today, JSON.stringify(data)]
        );
        return true;
      }
      return false;
    }));
    for (let j = 0; j < results.length; j++) {
      const r = results[j];
      if (r.status === "fulfilled" && r.value) success++;
      else {
        failed++;
        if (r.status === "rejected") log.warn("CHAMP_SNAP", `Failed ${batch[j]?.champion_id}/${batch[j]?.role}: ${r.reason?.message?.slice(0, 80)}`);
      }
    }
    if ((i + PARALLEL) % 100 < PARALLEL) log.info("CHAMP_SNAP", `Base: ${Math.min(i + PARALLEL, combos.length)}/${combos.length}`);
  }
  log.info("CHAMP_SNAP", `Base done: ${success} ok, ${failed} failed`);

  // ── Step 4: Tier-filtered snapshots ──
  log.info("CHAMP_SNAP", "=== Tier-filtered snapshots ===");
  const tierFilters = TIER_FILTERS.filter(t => t !== null) as string[];
  let tierSuccess = 0;
  let tierFailed = 0;

  for (const tier of tierFilters) {
    const t1 = Date.now();

    // Get puuids for this tier
    const { rows: tierPuuids } = await client.query(
      `SELECT array_agg(puuid) as puuids FROM users WHERE split_part(rank, ' ', 1) = ANY(tier_filter_ranks($1))`,
      [tier]
    );
    const puuids = tierPuuids[0]?.puuids;
    if (!puuids || puuids.length === 0) {
      log.info("CHAMP_SNAP", `Tier ${tier}: no players, skipping`);
      continue;
    }

    // Batch compute core stats
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

    // Batch compute items for this tier
    const { rows: tierItems } = await client.query(`
      WITH all_items AS (
        SELECT p.champion_id, p.role,
          unnest(ARRAY[p.item0, p.item1, p.item2, p.item3, p.item4, p.item5]) AS iid,
          p.win
        FROM participants p
        WHERE p.puuid = ANY($1)
          AND p.role IS NOT NULL AND p.role != '' AND p.role != 'Invalid'
      ),
      counts AS (
        SELECT champion_id, role, count(*) AS total
        FROM participants
        WHERE puuid = ANY($1) AND role IS NOT NULL AND role != '' AND role != 'Invalid'
        GROUP BY champion_id, role
      ),
      legendary AS (
        SELECT ai.champion_id, ai.role, ai.iid AS item_id, ai.win
        FROM all_items ai JOIN items i ON i.item_id = ai.iid
        WHERE i.is_legendary = true
      )
      SELECT l.champion_id, l.role, l.item_id,
        count(*)::int AS games, SUM(l.win::int)::int AS wins,
        ROUND(100.0 * SUM(l.win::int)::numeric / count(*), 2) AS winrate,
        ROUND(100.0 * count(*)::numeric / NULLIF(c.total, 0), 2) AS pick_rate
      FROM legendary l
      JOIN counts c ON c.champion_id = l.champion_id AND c.role = l.role
      GROUP BY l.champion_id, l.role, l.item_id, c.total
      HAVING count(*) >= 3
      ORDER BY l.champion_id, l.role, count(*) DESC
    `, [puuids]);

    // Build tier item map
    const tierItemMap = new Map<string, any[]>();
    for (const row of tierItems) {
      const key = `${row.champion_id}:${row.role}`;
      if (!tierItemMap.has(key)) tierItemMap.set(key, []);
      const arr = tierItemMap.get(key)!;
      if (arr.length < 12) {
        arr.push({
          item_id: row.item_id, games: row.games, wins: row.wins,
          winrate: Number(row.winrate), pick_rate: Number(row.pick_rate),
        });
      }
    }

    log.info("CHAMP_SNAP", `Tier ${tier}: ${tierStats.length} combos, ${tierItems.length} item rows in ${Date.now() - t1}ms`);

    for (const stat of tierStats) {
      try {
        const baseSnap = await client.query(
          `SELECT data FROM champion_stats_snapshots WHERE champion_id = $1 AND role = $2 AND snapshot_date = $3 AND tier IS NULL`,
          [stat.champion_id, stat.role, today]
        );
        const baseData = baseSnap.rows[0]?.data;

        const data = {
          core: {
            winrate: Number(stat.winrate), pickrate: null, banrate: null,
            gamesAnalyzed: stat.games,
            avgKDA: { kills: Number(stat.avg_kills), deaths: Number(stat.avg_deaths), assists: Number(stat.avg_assists) },
            avgCS: null, avgGold: Number(stat.avg_gold), avgDamage: Number(stat.avg_dmg),
          },
          bestMatchups: baseData?.bestMatchups ?? [],
          worstMatchups: baseData?.worstMatchups ?? [],
          bestSynergies: baseData?.bestSynergies ?? [],
          worstCounters: baseData?.worstCounters ?? [],
          objectiveWinrates: baseData?.objectiveWinrates ?? { firstDragon: null, firstBaron: null },
          gamePhaseWinrates: baseData?.gamePhaseWinrates ?? [],
          items: tierItemMap.get(`${stat.champion_id}:${stat.role}`) ?? [],
          meta: { queueId: 420, role: stat.role, tier, lastUpdatedUtc: new Date().toISOString() },
        };

        await client.query(
          `INSERT INTO champion_stats_snapshots (champion_id, role, snapshot_date, tier, data)
           VALUES ($1, $2, $3, $4, $5) ON CONFLICT (champion_id, role, snapshot_date, tier) DO UPDATE SET data = EXCLUDED.data`,
          [stat.champion_id, stat.role, today, tier, JSON.stringify(data)]
        );
        tierSuccess++;
      } catch { tierFailed++; }
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
