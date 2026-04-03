-- ============================================================
-- Migration: Match Ingestion Overhaul
-- Adds early game stats + missing fields to participants,
-- performance indexes, and truncates old inconsistent data.
-- ============================================================

-- ── 1. Add new columns to participants ──────────────────────

ALTER TABLE participants
  ADD COLUMN IF NOT EXISTS total_cs integer,
  ADD COLUMN IF NOT EXISTS total_minions_killed integer,
  ADD COLUMN IF NOT EXISTS neutral_minions_killed integer,
  ADD COLUMN IF NOT EXISTS champ_level integer,
  ADD COLUMN IF NOT EXISTS time_played integer,
  ADD COLUMN IF NOT EXISTS solo_kills integer,
  ADD COLUMN IF NOT EXISTS summoner1_id integer,
  ADD COLUMN IF NOT EXISTS summoner2_id integer,
  ADD COLUMN IF NOT EXISTS gold_at_10 integer,
  ADD COLUMN IF NOT EXISTS cs_at_10 integer,
  ADD COLUMN IF NOT EXISTS xp_at_10 integer,
  ADD COLUMN IF NOT EXISTS kills_at_10 smallint,
  ADD COLUMN IF NOT EXISTS deaths_at_10 smallint,
  ADD COLUMN IF NOT EXISTS assists_at_10 smallint,
  ADD COLUMN IF NOT EXISTS kill_participation real,
  ADD COLUMN IF NOT EXISTS damage_share real,
  ADD COLUMN IF NOT EXISTS riot_id_game_name text,
  ADD COLUMN IF NOT EXISTS riot_id_tagline text;

-- ── 2. Performance indexes ──────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_participants_puuid ON participants(puuid);
CREATE INDEX IF NOT EXISTS idx_participants_puuid_match ON participants(puuid, match_id);
CREATE INDEX IF NOT EXISTS idx_matches_creation_desc ON matches(game_creation DESC);
CREATE INDEX IF NOT EXISTS idx_matches_queue ON matches(queue_id);
CREATE INDEX IF NOT EXISTS idx_participants_champion ON participants(champion_id);
CREATE INDEX IF NOT EXISTS idx_participants_champion_role ON participants(champion_id, role);

-- ── 3. Truncate old inconsistent data (dependency order) ────
-- Run this AFTER deploying cron changes, BEFORE the backfill.
-- Uncomment when ready:

-- TRUNCATE participant_item_events, match_objective_events,
--   season_processed_matches, season_champion_matchups,
--   season_champion_aggregates, season_aggregates,
--   participants, match_teams, matches;
