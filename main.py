import os
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

# Load env
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
RIOT_API_KEY = os.getenv("RIOT_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

QUEUE_TYPES = {
    420: "RANKED_SOLO_DUO",
    440: "RANKED_FLEX"
}

def get_puuid_and_summoner_id(nametag: str):
    if "#" not in nametag:
        return None, None
    game_name, tag_line = nametag.split("#")
    
    # Account info
    account_url = f"https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}?api_key={RIOT_API_KEY}"
    acc_res = requests.get(account_url)
    if acc_res.status_code != 200:
        print(f"[{nametag}] Errore account: {acc_res.status_code}")
        return None, None
    puuid = acc_res.json().get("puuid")
    
    # Summoner info
    summoner_url = f"https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}?api_key={RIOT_API_KEY}"
    summoner_res = requests.get(summoner_url)
    if summoner_res.status_code != 200:
        print(f"[{nametag}] Errore summoner: {summoner_res.status_code}")
        return puuid, None
    summoner_id = summoner_res.json().get("id")
    return puuid, summoner_id

def get_rank_info_by_puuid(puuid: str):
    url = f"https://euw1.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}?api_key={RIOT_API_KEY}"
    res = requests.get(url)
    if res.status_code != 200:
        print(f"[Rank] Errore {res.status_code} - {res.text}")
        return None, None

    entries = res.json()
    if not entries:
        print(f"[Rank] Nessuna entry trovata per puuid: {puuid}")
        return None, None

    # Priorità: SOLO → FLEX
    solo = next((e for e in entries if e["queueType"] == "RANKED_SOLO_5x5"), None)
    flex = next((e for e in entries if e["queueType"] == "RANKED_FLEX_SR"), None)

    entry = solo or flex
    if entry:
        rank = f"{entry['tier']} {entry['rank']}"
        lp = entry['leaguePoints']
        return rank, lp

    return None, None

def get_latest_match_id(puuid: str):
    url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count=1&api_key={RIOT_API_KEY}"
    res = requests.get(url)
    return res.json()[0] if res.status_code == 200 and res.json() else None

def get_match_details(match_id: str):
    url = f"https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={RIOT_API_KEY}"
    res = requests.get(url)
    return res.json() if res.status_code == 200 else None

def process_users():
    response = supabase.table("profile_players").select("nametag").eq("premium", True).execute()
    if not response.data:
        print("Nessun utente premium.")
        return

    for user in response.data:
        nametag = user["nametag"]
        puuid, summoner_id = get_puuid_and_summoner_id(nametag)
        if not puuid:
            continue

        match_id = get_latest_match_id(puuid)
        if not match_id:
            print(f"[{nametag}] Nessun match trovato.")
            continue

        # Salta se già esiste
        existing = supabase.table("tracked_games").select("id").eq("match_id", match_id).eq("nametag", nametag).execute()
        if existing.data:
            print(f"[{nametag}] Match {match_id} già salvato.")
            continue

        match = get_match_details(match_id)
        if not match:
            continue

        queue_id = match['info']['queueId']
        if queue_id not in QUEUE_TYPES:
            print(f"[{nametag}] Match {match_id} non ranked.")
            continue

        participants = match['info']['participants']
        player = next((p for p in participants if p['puuid'] == puuid), None)
        if not player:
            continue

        lane = player['teamPosition']
        champ = player['championName']
        team = player['teamId']

        # Matchup: altro champ nella stessa lane
        matchup = next(
            (p['championName'] for p in participants if p['teamPosition'] == lane and p['teamId'] != team),
            None
        )

        rank, lp = get_rank_info_by_puuid(puuid)

        print(f"[{nametag}] → Match: {match_id} | Champ: {champ} | Lane: {lane} | vs: {matchup} | Rank: {rank} | LP: {lp}")

        data = {
            "nametag": nametag,
            "match_id": match_id,
            "champion_name": champ,
            "lane": lane,
            "matchup": matchup,
            "queue_type": QUEUE_TYPES[queue_id],
            "rank": rank,
            "lp": lp
        }

        supabase.table("tracked_games").insert(data).execute()
        print(f"[{nametag}] Match salvato.")

if __name__ == "__main__":
    process_users()
