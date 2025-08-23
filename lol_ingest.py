# lol_ingest.py
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
from supabase import Client

EUROPE = "https://europe.api.riotgames.com"
EUW1   = "https://euw1.api.riotgames.com"

# ---------------- Tuning (override da main se vuoi) ----------------
MAX_MATCHES_PER_SEED = 5          # quante partite per PUUID
MAX_FANOUT_PUUIDS    = 10         # usato dalla BFS classica
BFS_DEPTH            = 1
SLEEP_BETWEEN_CALLS  = 1.1        # pausa tra chiamate Riot

RANK_CACHE_TTL_DAYS  = 7
PREFERRED_QUEUE_TYPE = "RANKED_SOLO_5x5"

# Budget per “finestra” + cooldown quando esaurito (loop NON termina)
API_BUDGET_CALLS     = 180
BUDGET_COOLDOWN_SEC  = 90
_calls_used = 0

# Revisit/queue infinito
REVISIT_INTERVAL_MIN = 30         # dopo quanti minuti un PUUID va riprocessato
REQUEUE_BATCH        = 50         # quanti PUUID riattivare per giro
EMPTY_QUEUE_SLEEP    = 15         # attesa quando la coda è vuota
POST_PUUID_PAUSE     = 0.5        # pausa tra PUUID per anti-burst

# ---------------- Helpers ----------------
def to_ts_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

def parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def riot_get(url: str, api_key: str, params: dict | None = None):
    """
    GET con gestione budget, cooldown automatico, 429 e 5xx.
    NON ritorna mai definitivamente per budget: se finisce, attende e riprova.
    Ritorna None solo per errori hard (403/404 ecc).
    """
    global _calls_used
    if params is None:
        params = {}
    params["api_key"] = api_key

    while True:
        # finestra budget: se finita, cooldown e reset
        if _calls_used >= API_BUDGET_CALLS:
            print(f"[BUDGET] Cooling down {BUDGET_COOLDOWN_SEC}s…")
            time.sleep(BUDGET_COOLDOWN_SEC)
            _calls_used = 0

        _calls_used += 1
        try:
            res = requests.get(url, params=params, timeout=15)
        except requests.RequestException as e:
            print(f"[RIOT] Request error: {e}; retry 2s")
            time.sleep(2)
            continue

        if res.status_code == 429:
            retry = int(res.headers.get("Retry-After", "2"))
            print(f"[RIOT] 429 rate limited, retry {retry}s")
            time.sleep(retry + 1)
            continue

        if 500 <= res.status_code < 600:
            print(f"[RIOT] {res.status_code} server error, retry 2s")
            time.sleep(2)
            continue

        if res.status_code != 200:
            # errore “hard” (404 match not found, 403 key, ecc.) → non loop infinito
            print(f"[RIOT] {res.status_code} {url} {res.text[:160]}")
            return None

        time.sleep(SLEEP_BETWEEN_CALLS)
        return res.json()

def derive_platform_from_match_id(match_id: str) -> str:
    return match_id.split("_", 1)[0].lower()

def robust_game_duration_seconds(info: dict) -> int:
    start = info.get("gameStartTimestamp")
    end   = info.get("gameEndTimestamp")
    if start and end and end > start:
        return int((end - start) / 1000)
    gd = info.get("gameDuration")
    return int(gd) if gd else 0

# ---------------- Riot fetchers ----------------
def get_puuid_by_riot_id(nametag: str, api_key: str) -> Optional[str]:
    if "#" not in nametag:
        return None
    game_name, tag_line = nametag.split("#", 1)
    url = f"{EUROPE}/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    data = riot_get(url, api_key)
    return data.get("puuid") if data else None

def list_recent_match_ids(puuid: str, count: int, api_key: str) -> List[str]:
    url = f"{EUROPE}/lol/match/v5/matches/by-puuid/{puuid}/ids"
    data = riot_get(url, api_key, params={"start": 0, "count": count})
    return data or []

def fetch_match(match_id: str, api_key: str) -> Optional[Dict]:
    url = f"{EUROPE}/lol/match/v5/matches/{match_id}"
    return riot_get(url, api_key)

def fetch_timeline(match_id: str, api_key: str) -> Optional[Dict]:
    url = f"{EUROPE}/lol/match/v5/matches/{match_id}/timeline"
    return riot_get(url, api_key)

# ---------------- Upserts core ----------------
def upsert_match_core(supabase: Client, match_json: dict):
    meta = match_json["metadata"]
    info = match_json["info"]
    row = {
        "match_id": meta["matchId"],
        "platform": derive_platform_from_match_id(meta["matchId"]),
        "game_creation": to_ts_iso(info["gameCreation"]),
        "game_duration_seconds": robust_game_duration_seconds(info),
        "game_version": info.get("gameVersion"),
        "queue_id": info.get("queueId"),
        "raw": match_json,
    }
    supabase.table("matches").upsert(row).execute()

def upsert_match_teams(supabase: Client, match_json: dict):
    info = match_json["info"]
    rows = []
    for t in info.get("teams", []):
        rows.append({
            "match_id": match_json["metadata"]["matchId"],
            "team_id": t.get("teamId"),
            "win": bool(t.get("win")),
            "first_dragon": t.get("objectives", {}).get("dragon", {}).get("first"),
            "first_baron":  t.get("objectives", {}).get("baron", {}).get("first"),
            "towers_destroyed": t.get("objectives", {}).get("tower", {}).get("kills"),
            "dragons": t.get("objectives", {}).get("dragon", {}).get("kills"),
            "barons":  t.get("objectives", {}).get("baron", {}).get("kills"),
        })
    if rows:
        supabase.table("match_teams").upsert(rows, on_conflict="match_id,team_id").execute()

def upsert_participants(supabase: Client, match_json: dict):
    info = match_json["info"]
    match_id = match_json["metadata"]["matchId"]
    rows = []
    for p in info["participants"]:
        rows.append({
            "match_id": match_id,
            "participant_id": p["participantId"],
            "puuid": p["puuid"],
            "summoner_name": p.get("summonerName"),
            "team_id": p["teamId"],
            "champion_id": p.get("championId"),
            "champion_name": p.get("championName"),
            "role": p.get("teamPosition"),
            "lane": p.get("lane"),
            "win": bool(p.get("win")),
            "kills": p.get("kills"),
            "deaths": p.get("deaths"),
            "assists": p.get("assists"),
            "gold_earned": p.get("goldEarned"),
            "total_damage_to_champions": p.get("totalDamageDealtToChampions"),
            "vision_score": p.get("visionScore"),
            "item0": p.get("item0"),
            "item1": p.get("item1"),
            "item2": p.get("item2"),
            "item3": p.get("item3"),
            "item4": p.get("item4"),
            "item5": p.get("item5"),
            "item6": p.get("item6"),
        })
    supabase.table("participants").upsert(rows, on_conflict="match_id,participant_id").execute()

def upsert_timeline_raw(supabase: Client, match_id: str, timeline_json: dict):
    supabase.table("match_timelines").upsert({
        "match_id": match_id,
        "raw": timeline_json
    }).execute()

def upsert_item_events_from_timeline(supabase: Client, match_id: str, timeline_json: dict):
    frames = timeline_json.get("info", {}).get("frames", [])
    unique: dict[tuple, dict] = {}

    for fr in frames:
        for ev in fr.get("events", []):
            etype = ev.get("type")
            if etype not in ("ITEM_PURCHASED", "ITEM_SOLD", "ITEM_UNDO", "ITEM_TRANSFORMED"):
                continue

            pid = ev.get("participantId")
            ts  = ev.get("timestamp")
            item_id = ev.get("itemId") or ev.get("afterId") or ev.get("transformId")
            if not pid or ts is None or not item_id or item_id <= 0:
                continue

            event_type = (
                "PURCHASE"  if etype == "ITEM_PURCHASED"  else
                "SELL"      if etype == "ITEM_SOLD"       else
                "UNDO"      if etype == "ITEM_UNDO"       else
                "TRANSFORM"
            )

            key = (match_id, int(pid), int(ts), event_type, int(item_id))
            if key in unique:
                continue

            unique[key] = {
                "match_id": match_id,
                "participant_id": int(pid),
                "puuid": None,
                "ts_ms": int(ts),
                "event_type": event_type,
                "item_id": int(item_id),
                "gold": None
            }

    rows = list(unique.values())
    if not rows:
        return

    CHUNK = 300
    for i in range(0, len(rows), CHUNK):
        supabase.table("participant_item_events").upsert(
            rows[i:i+CHUNK],
            on_conflict="match_id,participant_id,ts_ms,event_type,item_id",
            ignore_duplicates=True,
            returning="minimal"
        ).execute()

# ---------------- Rank cache + snapshot ----------------
def _pick_rank_entry(entries: list) -> Optional[dict]:
    solo = next((e for e in entries if e.get("queueType") == "RANKED_SOLO_5x5"), None)
    flex = next((e for e in entries if e.get("queueType") == "RANKED_FLEX_SR"), None)
    return solo or flex

def get_rank_for_puuid_cached(supabase: Client, api_key: str, puuid: str) -> Optional[dict]:
    """Ritorna {'puuid','tier','queue_type','fetched_at'} oppure None. Aggiorna cache se scaduta."""
    rec = supabase.table("rank_cache").select("*").eq("puuid", puuid).execute()
    cached = (rec.data or [None])[0]
    fresh_enough = False
    if cached and cached.get("fetched_at"):
        try:
            ts = parse_iso(cached["fetched_at"])
            fresh_enough = (datetime.now(timezone.utc) - ts) < timedelta(days=RANK_CACHE_TTL_DAYS)
        except Exception:
            fresh_enough = False

    if cached and fresh_enough:
        return cached

    url = f"{EUW1}/lol/league/v4/entries/by-puuid/{puuid}"
    entries = riot_get(url, api_key) or []
    entry = _pick_rank_entry(entries)
    now_iso = now_utc_iso()

    if not entry:
        row = {"puuid": puuid, "tier": None, "queue_type": None, "fetched_at": now_iso}
        supabase.table("rank_cache").upsert(row).execute()
        return None

    row = {
        "puuid": puuid,
        "tier": entry.get("tier"),           # GOLD / MASTER / etc.
        "queue_type": entry.get("queueType"),
        "fetched_at": now_iso
    }
    supabase.table("rank_cache").upsert(row).execute()
    return row

def upsert_participant_ranks_for_match(supabase: Client, api_key: str, match_json: dict):
    match_id = match_json["metadata"]["matchId"]
    rows = []
    for p in match_json["info"]["participants"]:
        puuid = p["puuid"]
        rc = get_rank_for_puuid_cached(supabase, api_key, puuid)
        if not rc:
            continue
        rows.append({
            "match_id": match_id,
            "participant_id": p["participantId"],
            "puuid": puuid,
            "queue_type": rc["queue_type"],
            "tier": rc["tier"],
            "created_at": now_utc_iso()
        })

    if rows:
        supabase.table("participant_ranks").upsert(rows, on_conflict="match_id,participant_id").execute()


# ---------------- Summoner name cache & hydrator ----------------
def get_names_for_puuids_cached(supabase: Client, api_key: str, puuids: list[str]) -> dict[str, dict]:
    """
    Ritorna un dict {puuid: {"summoner_name": str|None, "game_name": str|None, "tag_line": str|None}}
    usando una cache locale; per i mancanti chiama:
      - euw1/summoner/v4/summoners/by-puuid -> 'name' (Summoner Name)
      - europe/riot/account/v1/accounts/by-puuid -> 'gameName','tagLine' (Riot ID)
    """
    if not puuids:
        return {}

    out: dict[str, dict] = {p: {"summoner_name": None, "game_name": None, "tag_line": None} for p in puuids}

    # 1) prova dalla cache
    res = (
        supabase.table("summoner_cache")
        .select("puuid,summoner_name,game_name,tag_line")
        .in_("puuid", puuids)
        .execute()
    )
    for row in (res.data or []):
        out[row["puuid"]] = {
            "summoner_name": row.get("summoner_name"),
            "game_name": row.get("game_name"),
            "tag_line": row.get("tag_line"),
        }

    missing = [p for p, v in out.items() if not (v["summoner_name"] or v["game_name"])]
    if not missing:
        return out

    # 2) per i mancanti chiamiamo le API (con il nostro riot_get che gestisce budget/cooldown)
    for puuid in missing:
        # Summoner-V4 (nome invocatore lato piattaforma)
        summ = riot_get(f"{EUW1}/lol/summoner/v4/summoners/by-puuid/{puuid}", api_key)
        if summ and isinstance(summ, dict):
            out[puuid]["summoner_name"] = summ.get("name")

        # Account-V1 (Riot ID)
        acc = riot_get(f"{EUROPE}/riot/account/v1/accounts/by-puuid/{puuid}", api_key)
        if acc and isinstance(acc, dict):
            out[puuid]["game_name"] = acc.get("gameName")
            out[puuid]["tag_line"]  = acc.get("tagLine")

        # upsert in cache
        supabase.table("summoner_cache").upsert({
            "puuid": puuid,
            "summoner_name": out[puuid]["summoner_name"],
            "game_name": out[puuid]["game_name"],
            "tag_line": out[puuid]["tag_line"],
            "fetched_at": now_utc_iso()
        }).execute()

    return out


def hydrate_participants_names(supabase: Client, api_key: str, match_json: dict):
    """
    Se 'participants.summoner_name' è NULL/vuoto (tipico di match-v5), riempilo usando la cache.
    Aggiorna solo le righe vuote, per non fare update inutili.
    """
    match_id = match_json["metadata"]["matchId"]
    puuids = [p["puuid"] for p in match_json["info"]["participants"] if p.get("puuid")]
    if not puuids:
        return

    names = get_names_for_puuids_cached(supabase, api_key, puuids)

    # Aggiorna solo dove summoner_name è NULL o '' per questo match
    for puuid in puuids:
        sname = names.get(puuid, {}).get("summoner_name")
        if not sname:
            # se non abbiamo Summoner Name, puoi decidere di salvare "game_name#tag_line"
            gn = names.get(puuid, {}).get("game_name")
            tl = names.get(puuid, {}).get("tag_line")
            if gn and tl:
                sname = f"{gn}#{tl}"
        if not sname:
            continue

        supabase.table("participants").update(
            {"summoner_name": sname}
        ).eq("match_id", match_id).eq("puuid", puuid).is_("summoner_name", None).execute()

        # in caso la colonna abbia default '' invece di NULL:
        supabase.table("participants").update(
            {"summoner_name": sname}
        ).eq("match_id", match_id).eq("puuid", puuid).eq("summoner_name", "").execute()


# ---------------- Bundle ingest ----------------
def upsert_full_match_bundle(supabase: Client, api_key: str, match_id: str) -> Optional[Dict]:
    match_json = fetch_match(match_id, api_key)
    if not match_json:
        return None

    # salva solo ranked solo/duo o flex
    qid = match_json["info"].get("queueId")
    if qid not in (420, 440):
        print(f"[SKIP] Match {match_id} queue_id={qid} non ranked/flex")
        return None

    upsert_match_core(supabase, match_json)
    upsert_match_teams(supabase, match_json)
    upsert_participants(supabase, match_json)
    hydrate_participants_names(supabase, api_key, match_json)
    upsert_participant_ranks_for_match(supabase, api_key, match_json)

    timeline_json = fetch_timeline(match_id, api_key)
    if timeline_json:
        upsert_timeline_raw(supabase, match_json["metadata"]["matchId"], timeline_json)
        upsert_item_events_from_timeline(supabase, match_json["metadata"]["matchId"], timeline_json)

    return match_json

# ---------------- BFS classica (opzionale) ----------------
def bfs_seed(supabase: Client, api_key: str, nametag_seed: str):
    puuid = get_puuid_by_riot_id(nametag_seed, api_key)
    if not puuid:
        print(f"[SEED] Impossibile ottenere PUUID per {nametag_seed}")
        return

    visited = {puuid}
    frontier = [puuid]

    for depth in range(BFS_DEPTH + 1):
        next_frontier: List[str] = []
        for pu in frontier:
            mids = list_recent_match_ids(pu, MAX_MATCHES_PER_SEED, api_key)
            for mid in mids:
                bundle = upsert_full_match_bundle(supabase, api_key, mid)
                if not bundle:
                    continue
                if depth < BFS_DEPTH:
                    for p in bundle["info"]["participants"]:
                        p_puuid = p["puuid"]
                        if p_puuid not in visited:
                            visited.add(p_puuid)
                            next_frontier.append(p_puuid)
                            if len(next_frontier) >= MAX_FANOUT_PUUIDS:
                                break
            if depth < BFS_DEPTH and len(next_frontier) >= MAX_FANOUT_PUUIDS:
                break
        frontier = next_frontier
        if not frontier:
            break

# ---------------- Coda persistente & crawl infinito ----------------
def enqueue_puuids(supabase: Client, puuids: list[str]):
    """Accoda PUUID nuovi senza toccare processed_at se già presente (evita thrash)."""
    if not puuids:
        return
    rows = [{"puuid": p} for p in puuids]
    supabase.table("ingest_puuid_queue").upsert(
        rows, on_conflict="puuid", ignore_duplicates=True, returning="minimal"
    ).execute()

def enqueue_seed_by_riot_id(supabase: Client, api_key: str, nametag: str):
    puuid = get_puuid_by_riot_id(nametag, api_key)
    if puuid:
        enqueue_puuids(supabase, [puuid])

def pop_next_puuid(supabase: Client) -> Optional[str]:
    """
    Prende 1 PUUID con processed_at NULL e lo 'locka' subito impostando processed_at=now.
    (Race minima accettabile con 1 worker. Se vuoi lock atomico vero: usa RPC/SQL.)
    """
    res = (
        supabase.table("ingest_puuid_queue")
        .select("puuid")
        .is_("processed_at", None)
        .limit(1)
        .execute()
    )
    if not res.data:
        return None
    puuid = res.data[0]["puuid"]
    supabase.table("ingest_puuid_queue").update({
        "processed_at": now_utc_iso()
    }).eq("puuid", puuid).execute()
    return puuid

def requeue_stale_puuids(supabase: Client) -> int:
    """Rende processabili i PUUID già processati da > REVISIT_INTERVAL_MIN."""
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=REVISIT_INTERVAL_MIN)).isoformat()
    res = (
        supabase.table("ingest_puuid_queue")
        .select("puuid")
        .lt("processed_at", cutoff)
        .limit(REQUEUE_BATCH)
        .execute()
    )
    rows = res.data or []
    if not rows:
        return 0
    puuids = [r["puuid"] for r in rows]
    for p in puuids:
        supabase.table("ingest_puuid_queue").update({"processed_at": None}).eq("puuid", p).execute()
    return len(puuids)

def reseed_from_existing_data(supabase: Client, limit: int = 50) -> int:
    """Se proprio la coda si svuota e non ci sono stale, pesca PUUID da participants e accodali."""
    res = (
        supabase.table("participants")
        .select("puuid")
        .limit(limit)
        .execute()
    )
    rows = res.data or []
    puuids = sorted({r["puuid"] for r in rows if r.get("puuid")})
    enqueue_puuids(supabase, puuids)
    return len(puuids)

def crawl_forever(supabase: Client, api_key: str, seed_nametag: str):
    """
    Loop infinito: prende PUUID dalla coda, ingest ultimi N match, accoda compagni/avversari.
    NON termina: quando il budget finisce, riot_get va in cooldown e riprende.
    Requeue periodico dei PUUID “stale”; se tutto vuoto, reseed da dati esistenti.
    """
    enqueue_seed_by_riot_id(supabase, api_key, seed_nametag)

    while True:
        # riattiva stale
        requeued = requeue_stale_puuids(supabase)
        if requeued:
            print(f"[CRAWL] Requeued {requeued} stale PUUIDs")

        puuid = pop_next_puuid(supabase)
        if not puuid:
            # prova a reseedare dalla base se proprio non c'è nulla
            added = reseed_from_existing_data(supabase, limit=REQUEUE_BATCH)
            if added:
                print(f"[CRAWL] Reseeded {added} PUUIDs from existing data")
                continue
            print(f"[CRAWL] Coda vuota, sleep {EMPTY_QUEUE_SLEEP}s...")
            time.sleep(EMPTY_QUEUE_SLEEP)
            continue

        print(f"[CRAWL] Process PUUID={puuid}")

        # prendi gli ultimi match (se budget finito, riot_get aspetta e poi continua)
        mids = list_recent_match_ids(puuid, MAX_MATCHES_PER_SEED, api_key) or []

        for mid in mids:
            bundle = upsert_full_match_bundle(supabase, api_key, mid)
            if bundle:
                others = [p["puuid"] for p in bundle["info"]["participants"]]
                enqueue_puuids(supabase, others)

        # già “lockato” all’uscita di pop_next_puuid → niente da aggiornare qui

        # piccolo respiro tra PUUID (tuning anti-burst)
        time.sleep(POST_PUUID_PAUSE)
