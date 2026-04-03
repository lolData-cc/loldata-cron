export type QueueApi = "RANKED_SOLO_5x5" | "RANKED_FLEX_SR";

export type LadderEntry = {
  summonerId?: string;
  puuid?: string;
  leaguePoints: number;
  wins: number;
  losses: number;
  hotStreak: boolean;
  veteran: boolean;
  freshBlood: boolean;
  inactive: boolean;
  tier: "CHALLENGER" | "GRANDMASTER" | "MASTER";
};

export type RankedEntry = {
  queueType: string;
  tier: string;
  rank: string;
  leaguePoints: number;
  wins: number;
  losses: number;
};

/** Entry returned by /lol/league/v4/entries/{queue}/{tier}/{division}?page={n} */
export type LeagueEntryDTO = {
  summonerId: string;
  puuid: string;
  tier: string;
  rank: string;       // Division: "I" | "II" | "III" | "IV"
  leaguePoints: number;
  wins: number;
  losses: number;
  hotStreak: boolean;
  veteran: boolean;
  freshBlood: boolean;
  inactive: boolean;
};
