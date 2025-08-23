import os
import requests
import argparse
from dotenv import load_dotenv
from supabase import create_client, Client
from lol_ingest import (
    bfs_seed,
    MAX_MATCHES_PER_SEED as DEFAULT_MAX_MATCHES_PER_SEED,
    BFS_DEPTH as DEFAULT_BFS_DEPTH,
    SLEEP_BETWEEN_CALLS as DEFAULT_SLEEP_BETWEEN_CALLS,
)

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
RIOT_API_KEY = os.getenv("RIOT_API_KEY")
assert SUPABASE_URL and SUPABASE_KEY and RIOT_API_KEY, "SUPABASE_URL/SUPABASE_KEY/RIOT_API_KEY mancanti"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

QUEUE_TYPES = {420: "RANKED_SOLO_DUO", 440: "RANKED_FLEX"}


def get_puuid_and_summoner_id(nametag: str):
    if "#" not in nametag:
        return None, None
    game_name, tag_line = nametag.split("#")
    acc = requests.get(
        f"https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}",
        params={"api_key": RIOT_API_KEY},
        timeout=15,
    )
    if acc.status_code != 200:
        return None, None
    puuid = acc.json().get("puuid")
    summ = requests.get(
        f"https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}",
        params={"api_key": RIOT_API_KEY},
        timeout=15,
    )
    if summ.status_code != 200:
        return puuid, None
    return puuid, summ.json().get("id")


def get_rank_info_by_puuid(puuid: str):
    r = requests.get(
        f"https://euw1.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}",
        params={"api_key": RIOT_API_KEY},
        timeout=15,
    )
    if r.status_code != 200:
        return None, None
    entries = r.json() or []
    solo = next((e for e in entries if e["queueType"] == "RANKED_SOLO_5x5"), None)
    flex = next((e for e in entries if e["queueType"] == "RANKED_FLEX_SR"), None)
    e = solo or flex
    return (f"{e['tier']} {e['rank']}", e["leaguePoints"]) if e else (None, None)


def get_latest_match_id(puuid: str):
    r = requests.get(
        f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids",
        params={"start": 0, "count": 1, "api_key": RIOT_API_KEY},
        timeout=15,
    )
    return r.json()[0] if r.status_code == 200 and r.json() else None


def get_match_details(match_id: str):
    r = requests.get(
        f"https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}",
        params={"api_key": RIOT_API_KEY},
        timeout=15,
    )
    return r.json() if r.status_code == 200 else None


def process_users():
    resp = (
        supabase.table("profile_players")
        .select("nametag, plan")
        .in_("plan", ["premium", "elite"])
        .execute()
    )
    users = resp.data or []
    if not users:
        print("Nessun utente premium/elite.")
        return

    for user in users:
        nametag = user.get("nametag")
        if not nametag:
            continue
        puuid, _ = get_puuid_and_summoner_id(nametag)
        if not puuid:
            continue

        match_id = get_latest_match_id(puuid)
        if not match_id:
            print(f"[{nametag}] Nessun match trovato.")
            continue

        existing = (
            supabase.table("tracked_games")
            .select("id")
            .eq("match_id", match_id)
            .eq("nametag", nametag)
            .execute()
        )
        if existing.data:
            print(f"[{nametag}] Match {match_id} già salvato.")
            continue

        match = get_match_details(match_id)
        if not match:
            continue

        queue_id = match["info"]["queueId"]
        if queue_id not in QUEUE_TYPES:
            print(f"[{nametag}] Match {match_id} non ranked.")
            continue

        participants = match["info"]["participants"]
        player = next((p for p in participants if p["puuid"] == puuid), None)
        if not player:
            continue

        lane = player["teamPosition"]
        champ = player["championName"]
        team = player["teamId"]
        matchup = next(
            (p["championName"] for p in participants if p["teamPosition"] == lane and p["teamId"] != team),
            None,
        )
        rank, lp = get_rank_info_by_puuid(puuid)

        print(
            f"[{nametag}] → Match: {match_id} | Champ: {champ} | Lane: {lane} | "
            f"vs: {matchup} | Rank: {rank} | LP: {lp}"
        )
        supabase.table("tracked_games").insert(
            {
                "nametag": nametag,
                "match_id": match_id,
                "champion_name": champ,
                "lane": lane,
                "matchup": matchup,
                "queue_type": QUEUE_TYPES[queue_id],
                "rank": rank,
                "lp": lp,
            }
        ).execute()
        print(f"[{nametag}] Match salvato.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest LOL data")
    parser.add_argument("--seed", default="Wasureta#EUW")
    parser.add_argument("--depth", type=int, default=DEFAULT_BFS_DEPTH)
    parser.add_argument("--matches", type=int, default=DEFAULT_MAX_MATCHES_PER_SEED)
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_BETWEEN_CALLS)
    parser.add_argument("--also-process-users", action="store_true")
    parser.add_argument(
        "--no-ingest",
        action="store_true",
        help="Skippa completamente l’ingest (bfs/crawl) e NON avvia lol_ingest",
    )
    parser.add_argument(
        "--crawl-forever",
        action="store_true",
        help="Crawler infinito con coda persistente",
    )
    parser.add_argument(
        "--budget-calls",
        type=int,
        default=120,
        help="Budget massimo di chiamate Riot per run",
    )
    args = parser.parse_args()

    # Importa e configura lol_ingest solo se serve davvero
    if not args.no_ingest or args.crawl_forever:
        import lol_ingest  # lazy import
        # override da CLI
        lol_ingest.API_BUDGET_CALLS = args.budget_calls
        lol_ingest.MAX_MATCHES_PER_SEED = args.matches
        lol_ingest.BFS_DEPTH = args.depth
        lol_ingest.SLEEP_BETWEEN_CALLS = args.sleep

    if args.crawl_forever and not args.no_ingest:
        print(
            f"[CRAWL] seed={args.seed} matches/puuid={args.matches} budget_calls={args.budget_calls}"
        )
        lol_ingest.crawl_forever(supabase, RIOT_API_KEY, args.seed)
        raise SystemExit()

    if not args.no_ingest:
        print(
            f"[INGEST] BFS seed={args.seed} depth={args.depth} matches/seed={args.matches} sleep={args.sleep}"
        )
        bfs_seed(supabase, RIOT_API_KEY, args.seed)

    if args.also_process_users:
        print("[TRACKED] Avvio process_users()")
        process_users()

    print("[DONE]")
