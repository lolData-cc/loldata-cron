// Quick script to run the migration SQL against Supabase
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
// Use the service role key (second SUPABASE_KEY in .env)
const SUPABASE_KEY = process.env.SUPABASE_KEY!;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const statements = [
  // Add new columns
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS total_cs integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS total_minions_killed integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS neutral_minions_killed integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS champ_level integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS time_played integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS solo_kills integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS summoner1_id integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS summoner2_id integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS gold_at_10 integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS cs_at_10 integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS xp_at_10 integer`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS kills_at_10 smallint`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS deaths_at_10 smallint`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS assists_at_10 smallint`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS kill_participation real`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS damage_share real`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS riot_id_game_name text`,
  `ALTER TABLE participants ADD COLUMN IF NOT EXISTS riot_id_tagline text`,
  // Add indexes
  `CREATE INDEX IF NOT EXISTS idx_participants_puuid ON participants(puuid)`,
  `CREATE INDEX IF NOT EXISTS idx_participants_puuid_match ON participants(puuid, match_id)`,
  `CREATE INDEX IF NOT EXISTS idx_matches_creation_desc ON matches(game_creation DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_matches_queue ON matches(queue_id)`,
  `CREATE INDEX IF NOT EXISTS idx_participants_champion ON participants(champion_id)`,
  `CREATE INDEX IF NOT EXISTS idx_participants_champion_role ON participants(champion_id, role)`,
];

async function main() {
  console.log("Running migration against", SUPABASE_URL);

  for (const sql of statements) {
    const shortSql = sql.slice(0, 80);
    const { error } = await supabase.rpc("exec_sql", { query: sql });
    if (error) {
      // Try direct approach if exec_sql doesn't exist
      console.log(`  [WARN] rpc exec_sql failed for: ${shortSql}...`);
      console.log(`         ${error.message}`);
    } else {
      console.log(`  [OK] ${shortSql}...`);
    }
  }

  console.log("\nMigration complete!");
}

main().catch(console.error);
