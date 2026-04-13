// src/jobs/snapshot-champion-stats.ts
// Daily cron: pre-compute champion stats for each champion/role combo.
// Uses materialized views for speed — no heavy RPC calls.
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

async function main() {
  const startTime = Date.now();
  log.info("CHAMP_SNAP", "Starting champion stats snapshot...");

  const client = await pool.connect();
  await client.query("SET statement_timeout = '600s'");

  const today = new Date().toISOString().slice(0, 10);

  // Delete ALL old snapshots
  await client.query("DELETE FROM champion_stats_snapshots");
  log.info("CHAMP_SNAP", "Old snapshots deleted");

  // ── Step 0: Refresh critical materialized views only ──
  for (const mv of ["mv_champion_role_stats", "mv_lane_opponents", "mv_lane_matchups"]) {
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
  await client.query("SET statement_timeout = '600s'");

  // ── Step 1: Bulk compute ALL core stats from mv_champion_role_stats ──
  log.info("CHAMP_SNAP", "Computing core stats from materialized view...");
  const t0 = Date.now();

  const { rows: totalRow } = await client.query("SELECT COALESCE(SUM(games), 1) AS total FROM mv_champion_role_stats");
  const totalGames = Number(totalRow[0].total);

  const { rows: coreStats } = await client.query(`
    SELECT champion_id, role,
      SUM(games)::int AS games,
      CASE WHEN SUM(games) = 0 THEN 0
        ELSE ROUND(100.0 * SUM(wins)::numeric / SUM(games), 2) END AS winrate,
      ROUND(SUM(avg_kills * games) / NULLIF(SUM(games), 0), 2) AS avg_kills,
      ROUND(SUM(avg_deaths * games) / NULLIF(SUM(games), 0), 2) AS avg_deaths,
      ROUND(SUM(avg_assists * games) / NULLIF(SUM(games), 0), 2) AS avg_assists,
      ROUND(SUM(avg_gold * games) / NULLIF(SUM(games), 0), 0) AS avg_gold,
      ROUND(SUM(avg_dmg * games) / NULLIF(SUM(games), 0), 0) AS avg_dmg
    FROM mv_champion_role_stats
    GROUP BY champion_id, role
    HAVING SUM(games) >= 50
    ORDER BY champion_id, role
  `);
  log.info("CHAMP_SNAP", `Core stats: ${coreStats.length} combos in ${Date.now() - t0}ms`);

  // ── Step 2: Bulk compute runes ──
  log.info("CHAMP_SNAP", "Computing rune stats...");
  const t1 = Date.now();
  let runeRows: any[] = [];
  try {
    const res = await client.query(`
      SELECT p.champion_id, p.role,
        p.perk_keystone, p.perk_primary_style, p.perk_sub_style,
        count(*)::int AS games, SUM(p.win::int)::int AS wins,
        ROUND(100.0 * SUM(p.win::int)::numeric / count(*), 2) AS winrate
      FROM participants p
      WHERE p.role IS NOT NULL AND p.role != '' AND p.role != 'Invalid'
        AND p.perk_keystone IS NOT NULL AND p.perk_keystone > 0
      GROUP BY p.champion_id, p.role, p.perk_keystone, p.perk_primary_style, p.perk_sub_style
      HAVING count(*) >= 5
      ORDER BY p.champion_id, p.role, count(*) DESC
    `);
    runeRows = res.rows;
    log.info("CHAMP_SNAP", `Rune data: ${runeRows.length} rows in ${Date.now() - t1}ms`);
  } catch (e: any) {
    log.warn("CHAMP_SNAP", `Rune query failed: ${e.message?.slice(0, 100)}`);
  }

  // Build rune lookup
  const runeMap = new Map<string, any[]>();
  for (const r of runeRows) {
    const key = `${r.champion_id}:${r.role}`;
    if (!runeMap.has(key)) runeMap.set(key, []);
    const arr = runeMap.get(key)!;
    if (arr.length < 8) {
      const totalForCombo = coreStats.find((c: any) => c.champion_id === r.champion_id && c.role === r.role)?.games ?? 1;
      arr.push({
        perk_keystone: r.perk_keystone,
        perk_primary_style: r.perk_primary_style,
        perk_sub_style: r.perk_sub_style,
        games: r.games, wins: r.wins,
        winrate: Number(r.winrate),
        pick_rate: Number((100 * r.games / totalForCombo).toFixed(2)),
      });
    }
  }

  // ── Step 3: Bulk compute items ──
  log.info("CHAMP_SNAP", "Computing item stats...");
  const t2 = Date.now();
  let itemRows: any[] = [];
  try {
    const res = await client.query(`
      SELECT p.champion_id, p.role, i.item_id,
        count(*)::int AS games, SUM(p.win::int)::int AS wins,
        ROUND(100.0 * SUM(p.win::int)::numeric / count(*), 2) AS winrate
      FROM participants p
      CROSS JOIN LATERAL unnest(ARRAY[p.item0, p.item1, p.item2, p.item3, p.item4, p.item5]) AS i(item_id)
      JOIN items it ON it.item_id = i.item_id AND it.is_legendary = true
      WHERE p.role IS NOT NULL AND p.role != '' AND p.role != 'Invalid'
      GROUP BY p.champion_id, p.role, i.item_id
      HAVING count(*) >= 5
      ORDER BY p.champion_id, p.role, count(*) DESC
    `);
    itemRows = res.rows;
    log.info("CHAMP_SNAP", `Item data: ${itemRows.length} rows in ${Date.now() - t2}ms`);
  } catch (e: any) {
    log.warn("CHAMP_SNAP", `Item query failed: ${e.message?.slice(0, 100)}`);
  }

  // Build item lookup
  const itemMap = new Map<string, any[]>();
  for (const r of itemRows) {
    const key = `${r.champion_id}:${r.role}`;
    if (!itemMap.has(key)) itemMap.set(key, []);
    const arr = itemMap.get(key)!;
    if (arr.length < 12) {
      const totalForCombo = coreStats.find((c: any) => c.champion_id === r.champion_id && c.role === r.role)?.games ?? 1;
      arr.push({
        item_id: r.item_id, games: r.games, wins: r.wins,
        winrate: Number(r.winrate),
        pick_rate: Number((100 * r.games / totalForCombo).toFixed(2)),
      });
    }
  }

  // ── Step 4: Bulk compute matchups from mv_lane_matchups ──
  log.info("CHAMP_SNAP", "Computing matchup stats...");
  const t3 = Date.now();
  let matchupRows: any[] = [];
  try {
    const res = await client.query(`
      SELECT champion_id, role, opponent_id,
        SUM(games)::int AS games, SUM(wins)::int AS wins,
        ROUND(100.0 * SUM(wins)::numeric / NULLIF(SUM(games), 0), 2) AS winrate
      FROM mv_lane_matchups
      GROUP BY champion_id, role, opponent_id
      HAVING SUM(games) >= 5
    `);
    matchupRows = res.rows;
    log.info("CHAMP_SNAP", `Matchup data: ${matchupRows.length} rows in ${Date.now() - t3}ms`);
  } catch (e: any) {
    log.warn("CHAMP_SNAP", `Matchup query failed: ${e.message?.slice(0, 100)}`);
  }

  // Build matchup lookup
  const matchupMap = new Map<string, any[]>();
  for (const r of matchupRows) {
    const key = `${r.champion_id}:${r.role}`;
    if (!matchupMap.has(key)) matchupMap.set(key, []);
    matchupMap.get(key)!.push({
      championKey: r.opponent_id,
      winrate: Number(r.winrate),
      winrateShrunk: Number((100 * (r.wins + 10) / (r.games + 20)).toFixed(2)),
      games: r.games,
    });
  }

  // ── Step 5: Insert all snapshots ──
  log.info("CHAMP_SNAP", `=== Inserting ${coreStats.length} base snapshots ===`);
  let success = 0, failed = 0;

  for (let i = 0; i < coreStats.length; i++) {
    const s = coreStats[i];
    const key = `${s.champion_id}:${s.role}`;
    const matchups = matchupMap.get(key) ?? [];
    const sorted = [...matchups].sort((a, b) => b.winrateShrunk - a.winrateShrunk);

    const data = {
      core: {
        winrate: Number(s.winrate),
        pickrate: Number((100 * s.games / totalGames).toFixed(2)),
        banrate: null,
        gamesAnalyzed: s.games,
        avgKDA: { kills: Number(s.avg_kills), deaths: Number(s.avg_deaths), assists: Number(s.avg_assists) },
        avgCS: null,
        avgGold: Number(s.avg_gold),
        avgDamage: Number(s.avg_dmg),
      },
      bestMatchups: sorted.slice(-10).reverse().map((m: any) => ({ ...m, winrateShrunk: undefined })),
      worstMatchups: sorted.slice(0, 10).map((m: any) => ({ ...m, winrateShrunk: undefined })),
      runes: runeMap.get(key) ?? [],
      items: itemMap.get(key) ?? [],
    };

    try {
      await pool.query(
        `INSERT INTO champion_stats_snapshots (champion_id, role, snapshot_date, tier, data)
         VALUES ($1, $2, $3, NULL, $4)`,
        [s.champion_id, s.role, today, JSON.stringify(data)]
      );
      success++;
    } catch (e: any) {
      failed++;
      if (failed <= 5) log.warn("CHAMP_SNAP", `Insert failed ${s.champion_id}/${s.role}: ${e.message?.slice(0, 80)}`);
    }

    if ((i + 1) % 100 === 0) log.info("CHAMP_SNAP", `Inserted: ${i + 1}/${coreStats.length}`);
  }

  log.info("CHAMP_SNAP", `Base done: ${success} ok, ${failed} failed`);

  client.release();

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  log.info("JOB_END", `snapshot-champion-stats completed in ${elapsed}min`, { combos: coreStats.length, success, failed });

  await pool.end();
}

main().catch((err) => {
  log.error("CHAMP_SNAP", `Fatal: ${err.message}`);
  process.exit(1);
});
