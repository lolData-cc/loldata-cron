// src/jobs/archive-patch.ts
// Compresses an old patch's raw data into summary rows, then deletes the raw data.
// Preserves per-champion winrates, item winrates, rune combos, and tierlist.
import { config } from "dotenv";
config({ override: true });

import pg from "pg";
import { log } from "../logger";

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  log.error("ARCHIVE", "DATABASE_URL not set");
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 3,
  idleTimeoutMillis: 30000,
});

export async function archivePatch(patchToArchive?: string): Promise<void> {
  const startTime = Date.now();
  const client = await pool.connect();
  await client.query("SET statement_timeout = '600s'");

  try {
    // ── Step 1: Identify the patch to archive ──
    let patch = patchToArchive;
    if (!patch) {
      // Find the second-most-recent patch (current patch stays raw)
      const { rows } = await client.query(`
        SELECT DISTINCT LEFT(game_version, POSITION('.' IN SUBSTRING(game_version FROM POSITION('.' IN game_version) + 1)) + POSITION('.' IN game_version) - 1) AS short_patch,
          MAX(game_creation) AS latest
        FROM matches
        WHERE game_version IS NOT NULL
        GROUP BY short_patch
        ORDER BY latest DESC
        LIMIT 2
      `);
      if (rows.length < 2) {
        log.info("ARCHIVE", "Only one patch in DB, nothing to archive");
        return;
      }
      patch = rows[1].short_patch;
    }

    log.info("ARCHIVE", `Archiving patch ${patch}...`);

    // Check if already archived
    const { rows: existing } = await client.query(
      "SELECT 1 FROM patch_archives WHERE patch = $1 LIMIT 1", [patch]
    );
    if (existing.length > 0) {
      log.info("ARCHIVE", `Patch ${patch} already archived, skipping`);
      return;
    }

    // ── Step 2: Count matches for this patch ──
    const { rows: countRows } = await client.query(`
      SELECT count(*) AS cnt FROM matches WHERE game_version LIKE $1
    `, [`${patch}.%`]);
    const matchCount = Number(countRows[0].cnt);
    log.info("ARCHIVE", `Found ${matchCount} matches for patch ${patch}`);

    if (matchCount === 0) {
      log.info("ARCHIVE", "No matches to archive");
      return;
    }

    // ── Step 3: Compute champion/role stats ──
    log.info("ARCHIVE", "Computing champion stats...");
    const { rows: coreStats } = await client.query(`
      SELECT p.champion_id, p.role,
        count(*)::int AS games,
        SUM(p.win::int)::int AS wins,
        ROUND(100.0 * SUM(p.win::int)::numeric / count(*), 2) AS winrate,
        ROUND(AVG(p.kills)::numeric, 2) AS avg_kills,
        ROUND(AVG(p.deaths)::numeric, 2) AS avg_deaths,
        ROUND(AVG(p.assists)::numeric, 2) AS avg_assists,
        ROUND(AVG(p.gold_earned)::numeric, 0) AS avg_gold,
        ROUND(AVG(p.total_damage_to_champions)::numeric, 0) AS avg_dmg
      FROM participants p
      JOIN matches m ON m.match_id = p.match_id
      WHERE m.game_version LIKE $1
        AND p.role IS NOT NULL AND p.role != '' AND p.role != 'Invalid'
      GROUP BY p.champion_id, p.role
      HAVING count(*) >= 10
    `, [`${patch}.%`]);
    log.info("ARCHIVE", `Core stats: ${coreStats.length} champion/role combos`);

    // Total games for pickrate
    const totalGames = coreStats.reduce((sum: number, s: any) => sum + s.games, 0);

    // ── Step 4: Compute item stats ──
    log.info("ARCHIVE", "Computing item stats...");
    const { rows: itemStats } = await client.query(`
      SELECT p.champion_id, p.role, i.item_id,
        count(*)::int AS games, SUM(p.win::int)::int AS wins,
        ROUND(100.0 * SUM(p.win::int)::numeric / count(*), 2) AS winrate
      FROM participants p
      JOIN matches m ON m.match_id = p.match_id
      CROSS JOIN LATERAL unnest(ARRAY[p.item0, p.item1, p.item2, p.item3, p.item4, p.item5]) AS i(item_id)
      JOIN items it ON it.item_id = i.item_id AND it.is_legendary = true
      WHERE m.game_version LIKE $1
        AND p.role IS NOT NULL AND p.role != '' AND p.role != 'Invalid'
      GROUP BY p.champion_id, p.role, i.item_id
      HAVING count(*) >= 5
      ORDER BY p.champion_id, p.role, count(*) DESC
    `, [`${patch}.%`]);
    log.info("ARCHIVE", `Item stats: ${itemStats.length} rows`);

    // Build item map
    const itemMap = new Map<string, any[]>();
    for (const r of itemStats) {
      const key = `${r.champion_id}:${r.role}`;
      if (!itemMap.has(key)) itemMap.set(key, []);
      const arr = itemMap.get(key)!;
      if (arr.length < 12) {
        arr.push({ item_id: r.item_id, games: r.games, wins: r.wins, winrate: Number(r.winrate) });
      }
    }

    // ── Step 5: Compute rune stats ──
    log.info("ARCHIVE", "Computing rune stats...");
    const { rows: runeStats } = await client.query(`
      SELECT p.champion_id, p.role,
        p.perk_keystone, p.perk_primary_style, p.perk_sub_style,
        count(*)::int AS games, SUM(p.win::int)::int AS wins,
        ROUND(100.0 * SUM(p.win::int)::numeric / count(*), 2) AS winrate
      FROM participants p
      JOIN matches m ON m.match_id = p.match_id
      WHERE m.game_version LIKE $1
        AND p.role IS NOT NULL AND p.role != '' AND p.role != 'Invalid'
        AND p.perk_keystone IS NOT NULL AND p.perk_keystone > 0
      GROUP BY p.champion_id, p.role, p.perk_keystone, p.perk_primary_style, p.perk_sub_style
      HAVING count(*) >= 5
      ORDER BY p.champion_id, p.role, count(*) DESC
    `, [`${patch}.%`]);
    log.info("ARCHIVE", `Rune stats: ${runeStats.length} rows`);

    // Build rune map
    const runeMap = new Map<string, any[]>();
    for (const r of runeStats) {
      const key = `${r.champion_id}:${r.role}`;
      if (!runeMap.has(key)) runeMap.set(key, []);
      const arr = runeMap.get(key)!;
      if (arr.length < 5) {
        arr.push({
          keystone: r.perk_keystone, primary_style: r.perk_primary_style,
          sub_style: r.perk_sub_style, games: r.games, wins: r.wins, winrate: Number(r.winrate),
        });
      }
    }

    // ── Step 6: Insert patch_archives ──
    log.info("ARCHIVE", `Inserting ${coreStats.length} archive rows...`);
    let inserted = 0;
    for (const s of coreStats) {
      const key = `${s.champion_id}:${s.role}`;
      const data = {
        games: s.games,
        wins: s.wins,
        winrate: Number(s.winrate),
        pickrate: Number((100 * s.games / totalGames).toFixed(2)),
        avgKDA: { kills: Number(s.avg_kills), deaths: Number(s.avg_deaths), assists: Number(s.avg_assists) },
        avgGold: Number(s.avg_gold),
        avgDamage: Number(s.avg_dmg),
        items: itemMap.get(key) ?? [],
        runes: runeMap.get(key) ?? [],
      };
      await client.query(
        `INSERT INTO patch_archives (patch, champion_id, role, data) VALUES ($1, $2, $3, $4)
         ON CONFLICT (patch, champion_id, role) DO UPDATE SET data = EXCLUDED.data`,
        [patch, s.champion_id, s.role, JSON.stringify(data)]
      );
      inserted++;
    }
    log.info("ARCHIVE", `Inserted ${inserted} archive rows`);

    // ── Step 7: Archive tierlist ──
    log.info("ARCHIVE", "Archiving tierlist...");
    const roles = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"];
    for (const role of roles) {
      const champStats = coreStats.filter((s: any) => s.role === role);
      const tierData = champStats.map((s: any) => ({
        champion_id: s.champion_id,
        winrate: Number(s.winrate),
        pickrate: Number((100 * s.games / totalGames).toFixed(2)),
        games: s.games,
      }));
      if (tierData.length > 0) {
        await client.query(
          `INSERT INTO tierlist_archives (patch, role, data) VALUES ($1, $2, $3)
           ON CONFLICT (patch, role) DO UPDATE SET data = EXCLUDED.data`,
          [patch, role, JSON.stringify(tierData)]
        );
      }
    }
    log.info("ARCHIVE", "Tierlist archived");

    // ── Step 8: Delete raw data for this patch ──
    log.info("ARCHIVE", `Deleting raw data for patch ${patch}...`);

    // Get match IDs first
    const { rows: matchIds } = await client.query(
      `SELECT match_id FROM matches WHERE game_version LIKE $1`, [`${patch}.%`]
    );
    const ids = matchIds.map((r: any) => r.match_id);

    if (ids.length > 0) {
      // Delete in batches to avoid huge transactions
      const BATCH = 1000;
      for (let i = 0; i < ids.length; i += BATCH) {
        const batch = ids.slice(i, i + BATCH);
        await client.query(`DELETE FROM participants WHERE match_id = ANY($1)`, [batch]);
        await client.query(`DELETE FROM match_teams WHERE match_id = ANY($1)`, [batch]);
        await client.query(`DELETE FROM participant_item_events WHERE match_id = ANY($1)`, [batch]);
        await client.query(`DELETE FROM matches WHERE match_id = ANY($1)`, [batch]);
        if ((i + BATCH) % 5000 < BATCH) {
          log.info("ARCHIVE", `Deleted ${Math.min(i + BATCH, ids.length)}/${ids.length} matches`);
        }
      }
    }

    // NOTE: Do NOT delete from season_processed_matches — keeping these markers
    // prevents the ingestion from re-fetching archived matches.

    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    log.info("JOB_END", `archive-patch completed in ${elapsed}min`, {
      patch, champCombos: coreStats.length, matchesDeleted: ids.length,
    });

  } catch (e: any) {
    log.error("ARCHIVE", `Fatal: ${e.message}`);
    throw e;
  } finally {
    client.release();
  }
}

// Archive ALL old patches (everything except the current/latest)
async function archiveAllOldPatches(): Promise<void> {
  const { rows } = await pool.query(`
    SELECT SPLIT_PART(game_version, '.', 1) || '.' || SPLIT_PART(game_version, '.', 2) AS patch,
      count(*) AS matches
    FROM matches WHERE game_version IS NOT NULL
    GROUP BY 1 ORDER BY MAX(game_creation) DESC
  `);

  if (rows.length < 2) {
    log.info("ARCHIVE", "Only one patch in DB, nothing to archive");
    return;
  }

  const currentPatch = rows[0].patch;
  const oldPatches = rows.slice(1);
  log.info("ARCHIVE", `Current patch: ${currentPatch}. Archiving ${oldPatches.length} old patches...`);

  for (const { patch, matches } of oldPatches) {
    log.info("ARCHIVE", `── Archiving ${patch} (${matches} matches) ──`);
    try {
      await archivePatch(patch);
    } catch (e: any) {
      log.error("ARCHIVE", `Failed to archive ${patch}: ${e.message?.slice(0, 100)}`);
    }
  }

  log.info("ARCHIVE", `All ${oldPatches.length} old patches archived!`);
}

// Clean up raw data for already-archived patches
async function cleanupArchivedPatches(): Promise<void> {
  const { rows } = await pool.query(`
    SELECT SPLIT_PART(game_version, '.', 1) || '.' || SPLIT_PART(game_version, '.', 2) AS patch,
      count(*) AS matches
    FROM matches WHERE game_version IS NOT NULL
    GROUP BY 1 ORDER BY MAX(game_creation) DESC
  `);

  const currentPatch = rows[0]?.patch;
  const oldPatches = rows.filter((r: any) => r.patch !== currentPatch);

  for (const { patch, matches } of oldPatches) {
    // Check if archived
    const { rows: archived } = await pool.query("SELECT 1 FROM patch_archives WHERE patch = $1 LIMIT 1", [patch]);
    if (archived.length === 0) {
      log.info("CLEANUP", `${patch} not archived yet, skipping`);
      continue;
    }

    log.info("CLEANUP", `Deleting ${matches} leftover matches for archived patch ${patch}...`);
    const client = await pool.connect();
    await client.query("SET statement_timeout = '300s'");

    try {
      // Delete in smaller batches
      let deleted = 0;
      while (true) {
        const { rows: batch } = await client.query(
          `SELECT match_id FROM matches WHERE game_version LIKE $1 LIMIT 500`, [`${patch}.%`]
        );
        if (batch.length === 0) break;
        const ids = batch.map((r: any) => r.match_id);
        await client.query(`DELETE FROM participant_item_events WHERE match_id = ANY($1)`, [ids]);
        await client.query(`DELETE FROM participants WHERE match_id = ANY($1)`, [ids]);
        await client.query(`DELETE FROM match_teams WHERE match_id = ANY($1)`, [ids]);
        // Keep season_processed_matches to prevent re-ingestion
        await client.query(`DELETE FROM matches WHERE match_id = ANY($1)`, [ids]);
        deleted += ids.length;
        if (deleted % 2000 < 500) log.info("CLEANUP", `${patch}: deleted ${deleted}/${matches}`);
      }
      log.info("CLEANUP", `${patch}: done (${deleted} deleted)`);
    } catch (e: any) {
      log.error("CLEANUP", `${patch} cleanup failed: ${e.message?.slice(0, 100)}`);
    } finally {
      client.release();
    }
  }
  log.info("CLEANUP", "All archived patches cleaned up!");
}

// Auto-run when executed directly
const patchArg = process.argv[2];
let job: Promise<void>;
if (patchArg === "cleanup") {
  job = cleanupArchivedPatches();
} else if (patchArg) {
  job = archivePatch(patchArg);
} else {
  job = archiveAllOldPatches();
}
job.then(() => pool.end()).catch((e) => {
  console.error(e);
  process.exit(1);
});
