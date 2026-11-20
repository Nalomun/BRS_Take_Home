"""
MLB data pipeline -> SQLite.

Pulls teams, schedule, box scores, players and transactions from the MLB Stats API,
and pitch-level data from Baseball Savant (Statcast), cleans it, and loads it into
a SQLite database.

Usage:
    python pipeline.py --start 2026-08-01 --end 2026-08-03
    python pipeline.py --start 2026-08-01 --end 2026-08-03 --db mlb.db --fresh
"""

import argparse
import io
import json
import os
import sqlite3
import sys
import time
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

STATS_API = "https://statsapi.mlb.com/api/v1"
SAVANT_CSV = "https://baseballsavant.mlb.com/statcast_search/csv"
RAW_DIR = "raw"  # cache of completed-game responses (box scores, Statcast CSVs)

# Statuses that mean the game was actually played to completion.
PLAYED_STATES = ("Final", "Game Over", "Completed Early")


# --------------------------------------------------------------------------- #
# HTTP helpers
# --------------------------------------------------------------------------- #
def make_session():
    s = requests.Session()
    retry = Retry(total=4, backoff_factor=1.0,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "Mozilla/5.0 (mlb-take-home pipeline)"
    return s


SESSION = make_session()


def get_json(url, params=None, cache_file=None):
    """GET a JSON endpoint. Completed-game data never changes, so it can be cached."""
    if cache_file and os.path.exists(cache_file):
        with open(cache_file) as f:
            return json.load(f)
    r = SESSION.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if cache_file:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, "w") as f:
            json.dump(data, f)
    return data


def get_text(url, params=None, cache_file=None):
    if cache_file and os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return f.read()
    r = SESSION.get(url, params=params, timeout=60)
    r.raise_for_status()
    text = r.content.decode("utf-8-sig")  # Savant CSVs can start with a BOM
    if cache_file:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, "w", encoding="utf-8") as f:
            f.write(text)
    return text


# --------------------------------------------------------------------------- #
# Small cleaning helpers
# --------------------------------------------------------------------------- #
def g(d, *keys, default=None):
    """Safe nested get: g(game, 'teams', 'home', 'team', 'id')."""
    for k in keys:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d


def to_int(v):
    try:
        return int(v) if v not in (None, "", "-") else None
    except (TypeError, ValueError):
        return None


def to_float(v):
    try:
        if v in (None, "", "-") or pd.isna(v):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def ip_to_outs(ip):
    """MLB writes innings pitched as '5.2' meaning 5 and 2/3. Convert to outs (17)."""
    if ip in (None, ""):
        return None
    try:
        whole, _, frac = str(ip).partition(".")
        return int(whole) * 3 + (int(frac) if frac else 0)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #
SCHEMA = """
CREATE TABLE IF NOT EXISTS team (
    team_id            INTEGER PRIMARY KEY,   -- MLB Stats API team id
    name               TEXT NOT NULL,         -- 'Boston Red Sox'
    abbreviation       TEXT,                  -- 'BOS' (matches Statcast home_team/away_team)
    team_name          TEXT,                  -- 'Red Sox'
    location_name      TEXT,                  -- 'Boston'
    league_id          INTEGER,
    league_name        TEXT,
    division_id        INTEGER,
    division_name      TEXT,
    venue_id           INTEGER,
    venue_name         TEXT,
    first_year_of_play INTEGER,
    active             INTEGER
);

CREATE TABLE IF NOT EXISTS player (
    player_id          INTEGER PRIMARY KEY,   -- MLBAM id; same id Statcast uses for pitcher/batter
    full_name          TEXT NOT NULL,
    first_name         TEXT,
    last_name          TEXT,
    primary_number     TEXT,
    primary_position   TEXT,
    bats               TEXT,                  -- L / R / S
    throws             TEXT,                  -- L / R
    birth_date         TEXT,
    birth_country      TEXT,
    height             TEXT,
    weight             INTEGER,
    mlb_debut_date     TEXT,
    current_team_id    INTEGER,
    active             INTEGER
);

CREATE TABLE IF NOT EXISTS schedule (
    game_pk            INTEGER PRIMARY KEY,
    official_date      TEXT NOT NULL,         -- local game date (YYYY-MM-DD)
    game_datetime_utc  TEXT,
    season             INTEGER,
    game_type          TEXT,                  -- R regular, P postseason, S spring, ...
    status_code        TEXT,
    abstract_state     TEXT,                  -- Preview / Live / Final
    detailed_state     TEXT,                  -- Final, Postponed, Suspended, ...
    away_team_id       INTEGER,
    home_team_id       INTEGER,
    away_score         INTEGER,               -- NULL when the game has no score
    home_score         INTEGER,
    winning_team_id    INTEGER,
    venue_id           INTEGER,
    venue_name         TEXT,
    double_header      TEXT,                  -- N / Y / S
    game_number        INTEGER,               -- 1 or 2 in a doubleheader
    is_played          INTEGER NOT NULL       -- 1 if completed; box score/pitch data loaded only for these
);

CREATE TABLE IF NOT EXISTS box_score (
    game_pk            INTEGER NOT NULL REFERENCES schedule(game_pk),
    player_id          INTEGER NOT NULL REFERENCES player(player_id),
    team_id            INTEGER,
    side               TEXT,                  -- home / away
    position           TEXT,
    batting_order      INTEGER,               -- 100, 101 ... (hundreds = lineup slot, ones = sub)
    is_substitute      INTEGER,
    -- batting (MLB box score)
    ab INTEGER, r INTEGER, h INTEGER, doubles INTEGER, triples INTEGER, hr INTEGER,
    rbi INTEGER, bb INTEGER, so INTEGER, sb INTEGER, lob INTEGER,
    -- pitching (MLB box score)
    ip                 TEXT,                  -- as displayed, e.g. '5.2'
    outs_recorded      INTEGER,               -- numeric version of ip, e.g. 17
    p_h INTEGER, p_r INTEGER, p_er INTEGER, p_bb INTEGER, p_so INTEGER, p_hr INTEGER,
    batters_faced      INTEGER,
    pitches_thrown     INTEGER,               -- MLB's official count
    strikes            INTEGER,
    -- pitching (derived from Statcast PITCH table)
    sc_pitches         INTEGER,
    sc_avg_velo        REAL,
    sc_max_velo        REAL,
    sc_avg_spin        REAL,
    sc_whiffs          INTEGER,
    sc_pitch_mix       TEXT,                  -- JSON, e.g. {"FF": 41, "SL": 22}
    PRIMARY KEY (game_pk, player_id)
);

CREATE TABLE IF NOT EXISTS pitch (
    game_pk            INTEGER NOT NULL REFERENCES schedule(game_pk),
    at_bat_number      INTEGER NOT NULL,
    pitch_number       INTEGER NOT NULL,      -- pitch number within the plate appearance
    game_date          TEXT,
    pitcher_id         INTEGER REFERENCES player(player_id),
    batter_id          INTEGER REFERENCES player(player_id),
    inning             INTEGER,
    inning_topbot      TEXT,
    balls              INTEGER,
    strikes            INTEGER,
    outs_when_up       INTEGER,
    stand              TEXT,
    p_throws           TEXT,
    pitch_type         TEXT,
    pitch_name         TEXT,
    release_speed      REAL,
    effective_speed    REAL,
    release_spin_rate  REAL,
    release_extension  REAL,
    pfx_x              REAL,
    pfx_z              REAL,
    plate_x            REAL,
    plate_z            REAL,
    zone               INTEGER,
    type               TEXT,                  -- B / S / X
    description        TEXT,
    events             TEXT,
    launch_speed       REAL,
    launch_angle       REAL,
    hit_distance_sc    REAL,
    estimated_ba_using_speedangle REAL,
    delta_run_exp      REAL,
    home_team          TEXT,
    away_team          TEXT,
    PRIMARY KEY (game_pk, at_bat_number, pitch_number)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id     INTEGER PRIMARY KEY,
    transaction_date   TEXT,
    effective_date     TEXT,
    resolution_date    TEXT,
    type_code          TEXT,
    type_desc          TEXT,
    description        TEXT,
    player_id          INTEGER,               -- not every transaction names a player
    player_name        TEXT,
    from_team_id       INTEGER,
    from_team_name     TEXT,
    to_team_id         INTEGER,
    to_team_name       TEXT
);

-- Run log so every load is traceable.
CREATE TABLE IF NOT EXISTS load_log (
    run_at       TEXT,
    start_date   TEXT,
    end_date     TEXT,
    step         TEXT,
    rows_loaded  INTEGER,
    note         TEXT
);

CREATE INDEX IF NOT EXISTS ix_schedule_date   ON schedule(official_date);
CREATE INDEX IF NOT EXISTS ix_pitch_pitcher   ON pitch(game_pk, pitcher_id);
CREATE INDEX IF NOT EXISTS ix_pitch_batter    ON pitch(batter_id);
CREATE INDEX IF NOT EXISTS ix_box_player      ON box_score(player_id);
CREATE INDEX IF NOT EXISTS ix_txn_player      ON transactions(player_id);
CREATE INDEX IF NOT EXISTS ix_txn_date        ON transactions(transaction_date);
"""


def upsert(conn, table, rows):
    """INSERT OR REPLACE a list of dicts. Re-running the same dates is idempotent."""
    if not rows:
        return 0
    cols = list(rows[0].keys())
    sql = (f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) "
           f"VALUES ({', '.join('?' for _ in cols)})")
    conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
    return len(rows)


# --------------------------------------------------------------------------- #
# Extract + transform, one function per table
# --------------------------------------------------------------------------- #
def load_teams(conn):
    data = get_json(f"{STATS_API}/teams", params={"sportId": 1})
    rows = []
    for t in data.get("teams", []):
        rows.append({
            "team_id": t["id"],
            "name": t.get("name"),
            "abbreviation": t.get("abbreviation"),
            "team_name": t.get("teamName"),
            "location_name": t.get("locationName"),
            "league_id": g(t, "league", "id"),
            "league_name": g(t, "league", "name"),
            "division_id": g(t, "division", "id"),
            "division_name": g(t, "division", "name"),
            "venue_id": g(t, "venue", "id"),
            "venue_name": g(t, "venue", "name"),
            "first_year_of_play": to_int(t.get("firstYearOfPlay")),
            "active": int(bool(t.get("active"))),
        })
    return upsert(conn, "team", rows)


def load_schedule(conn, start, end):
    data = get_json(f"{STATS_API}/schedule",
                    params={"sportId": 1, "startDate": start, "endDate": end})
    by_pk = {}
    for day in data.get("dates", []):
        for gm in day.get("games", []):
            detailed = g(gm, "status", "detailedState") or ""
            played = int(g(gm, "status", "abstractGameState") == "Final"
                         and detailed.startswith(PLAYED_STATES))
            home_win = g(gm, "teams", "home", "isWinner")
            away_win = g(gm, "teams", "away", "isWinner")
            winner = (g(gm, "teams", "home", "team", "id") if home_win
                      else g(gm, "teams", "away", "team", "id") if away_win else None)
            row = {
                "game_pk": gm["gamePk"],
                "official_date": gm.get("officialDate") or day.get("date"),
                "game_datetime_utc": gm.get("gameDate"),
                "season": to_int(gm.get("season")),
                "game_type": gm.get("gameType"),
                "status_code": g(gm, "status", "statusCode"),
                "abstract_state": g(gm, "status", "abstractGameState"),
                "detailed_state": detailed,
                "away_team_id": g(gm, "teams", "away", "team", "id"),
                "home_team_id": g(gm, "teams", "home", "team", "id"),
                "away_score": to_int(g(gm, "teams", "away", "score")),
                "home_score": to_int(g(gm, "teams", "home", "score")),
                "winning_team_id": winner,
                "venue_id": g(gm, "venue", "id"),
                "venue_name": g(gm, "venue", "name"),
                "double_header": gm.get("doubleHeader"),
                "game_number": to_int(gm.get("gameNumber")),
                "is_played": played,
            }
            # A suspended/resumed game appears on two dates with the same gamePk.
            # Keep the completed entry if there is one, else the latest date.
            prev = by_pk.get(row["game_pk"])
            if prev is None or row["is_played"] >= prev["is_played"]:
                by_pk[row["game_pk"]] = row
    rows = list(by_pk.values())
    upsert(conn, "schedule", rows)
    return rows


BAT_MAP = {"ab": "atBats", "r": "runs", "h": "hits", "doubles": "doubles",
           "triples": "triples", "hr": "homeRuns", "rbi": "rbi", "bb": "baseOnBalls",
           "so": "strikeOuts", "sb": "stolenBases", "lob": "leftOnBase"}
PIT_MAP = {"p_h": "hits", "p_r": "runs", "p_er": "earnedRuns", "p_bb": "baseOnBalls",
           "p_so": "strikeOuts", "p_hr": "homeRuns", "batters_faced": "battersFaced",
           "strikes": "strikes"}


def load_box_score(conn, game_pk):
    data = get_json(f"{STATS_API}/game/{game_pk}/boxscore",
                    cache_file=os.path.join(RAW_DIR, "boxscore", f"{game_pk}.json"))
    rows = []
    for side in ("away", "home"):
        team_id = g(data, "teams", side, "team", "id")
        for p in (g(data, "teams", side, "players") or {}).values():
            bat = g(p, "stats", "batting") or {}
            pit = g(p, "stats", "pitching") or {}
            if not bat and not pit:
                continue  # on the roster but did not appear in the game
            row = {
                "game_pk": game_pk,
                "player_id": g(p, "person", "id"),
                "team_id": team_id,
                "side": side,
                "position": g(p, "position", "abbreviation"),
                "batting_order": to_int(p.get("battingOrder")),
                "is_substitute": int(bool(g(p, "gameStatus", "isSubstitute"))),
            }
            for col, key in BAT_MAP.items():
                row[col] = to_int(bat.get(key)) if bat else None
            for col, key in PIT_MAP.items():
                row[col] = to_int(pit.get(key)) if pit else None
            row["ip"] = pit.get("inningsPitched") if pit else None
            row["outs_recorded"] = (to_int(pit.get("outs")) or ip_to_outs(row["ip"])) if pit else None
            row["pitches_thrown"] = (to_int(pit.get("numberOfPitches", pit.get("pitchesThrown")))
                                     if pit else None)
            rows.append(row)
    return upsert(conn, "box_score", rows), {r["player_id"] for r in rows}


PITCH_COLS = {
    # target column: (source column, converter)
    "game_pk": ("game_pk", to_int), "at_bat_number": ("at_bat_number", to_int),
    "pitch_number": ("pitch_number", to_int), "game_date": ("game_date", str),
    "pitcher_id": ("pitcher", to_int), "batter_id": ("batter", to_int),
    "inning": ("inning", to_int), "inning_topbot": ("inning_topbot", str),
    "balls": ("balls", to_int), "strikes": ("strikes", to_int),
    "outs_when_up": ("outs_when_up", to_int), "stand": ("stand", str),
    "p_throws": ("p_throws", str), "pitch_type": ("pitch_type", str),
    "pitch_name": ("pitch_name", str), "release_speed": ("release_speed", to_float),
    "effective_speed": ("effective_speed", to_float),
    "release_spin_rate": ("release_spin_rate", to_float),
    "release_extension": ("release_extension", to_float),
    "pfx_x": ("pfx_x", to_float), "pfx_z": ("pfx_z", to_float),
    "plate_x": ("plate_x", to_float), "plate_z": ("plate_z", to_float),
    "zone": ("zone", to_int), "type": ("type", str), "description": ("description", str),
    "events": ("events", str), "launch_speed": ("launch_speed", to_float),
    "launch_angle": ("launch_angle", to_float), "hit_distance_sc": ("hit_distance_sc", to_float),
    "estimated_ba_using_speedangle": ("estimated_ba_using_speedangle", to_float),
    "delta_run_exp": ("delta_run_exp", to_float),
    "home_team": ("home_team", str), "away_team": ("away_team", str),
}


def clean_str(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None


def load_pitches(conn, game_pk):
    text = get_text(SAVANT_CSV,
                    params={"all": "true", "type": "details", "game_pk": game_pk},
                    cache_file=os.path.join(RAW_DIR, "statcast", f"{game_pk}.csv"))
    if not text.strip() or text.lstrip().startswith("<"):
        return 0, set(), "no Statcast data returned"
    df = pd.read_csv(io.StringIO(text), low_memory=False)
    if df.empty or "pitch_number" not in df.columns:
        return 0, set(), "no Statcast rows"
    rows, dropped = [], 0
    for rec in df.to_dict("records"):
        row = {}
        for col, (src, conv) in PITCH_COLS.items():
            v = rec.get(src)
            row[col] = clean_str(v) if conv is str else conv(v)
        if None in (row["game_pk"], row["at_bat_number"], row["pitch_number"]):
            dropped += 1  # can't key it, so it can't be stored reliably
            continue
        rows.append(row)
    # Savant occasionally repeats a pitch; the primary key keeps one copy.
    n_unique = len({(r["at_bat_number"], r["pitch_number"]) for r in rows})
    note = None
    if dropped or n_unique != len(rows):
        note = f"dropped {dropped} unkeyed rows, {len(rows) - n_unique} duplicate pitches"
    upsert(conn, "pitch", rows)
    ids = {r["pitcher_id"] for r in rows} | {r["batter_id"] for r in rows}
    return n_unique, {i for i in ids if i}, note


def load_players(conn, player_ids):
    ids = sorted(i for i in player_ids if i)
    rows = []
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        data = get_json(f"{STATS_API}/people",
                        params={"personIds": ",".join(map(str, chunk)),
                                "hydrate": "currentTeam"})
        for p in data.get("people", []):
            rows.append({
                "player_id": p["id"],
                "full_name": p.get("fullName"),
                "first_name": p.get("firstName") or p.get("useName"),
                "last_name": p.get("lastName"),
                "primary_number": p.get("primaryNumber"),
                "primary_position": g(p, "primaryPosition", "abbreviation"),
                "bats": g(p, "batSide", "code"),
                "throws": g(p, "pitchHand", "code"),
                "birth_date": p.get("birthDate"),
                "birth_country": p.get("birthCountry"),
                "height": p.get("height"),
                "weight": to_int(p.get("weight")),
                "mlb_debut_date": p.get("mlbDebutDate"),
                "current_team_id": g(p, "currentTeam", "id"),
                "active": int(bool(p.get("active"))),
            })
    return upsert(conn, "player", rows)


def load_transactions(conn, start, end):
    data = get_json(f"{STATS_API}/transactions",
                    params={"startDate": start, "endDate": end, "sportId": 1})
    rows, skipped = [], 0
    for t in data.get("transactions", []):
        if t.get("id") is None:
            skipped += 1
            continue
        rows.append({
            "transaction_id": t["id"],
            "transaction_date": t.get("date"),
            "effective_date": t.get("effectiveDate"),
            "resolution_date": t.get("resolutionDate"),
            "type_code": t.get("typeCode"),
            "type_desc": t.get("typeDesc"),
            "description": t.get("description"),
            "player_id": g(t, "person", "id"),
            "player_name": g(t, "person", "fullName"),
            "from_team_id": g(t, "fromTeam", "id"),
            "from_team_name": g(t, "fromTeam", "name"),
            "to_team_id": g(t, "toTeam", "id"),
            "to_team_name": g(t, "toTeam", "name"),
        })
    # The API can return the same transaction id more than once; keep one row per id.
    unique = {r["transaction_id"]: r for r in rows}
    return upsert(conn, "transactions", list(unique.values())), skipped, len(rows) - len(unique)


# Roll Statcast pitches up to one row per pitcher per game and attach to BOX_SCORE.
STATCAST_ROLLUP = """
WITH agg AS (
    SELECT game_pk, pitcher_id,
           COUNT(*)                         AS sc_pitches,
           ROUND(AVG(release_speed), 1)     AS sc_avg_velo,
           MAX(release_speed)               AS sc_max_velo,
           ROUND(AVG(release_spin_rate), 0) AS sc_avg_spin,
           SUM(description IN ('swinging_strike', 'swinging_strike_blocked')) AS sc_whiffs
    FROM pitch GROUP BY game_pk, pitcher_id
),
mix AS (
    SELECT game_pk, pitcher_id, json_group_object(pitch_type, n) AS sc_pitch_mix
    FROM (SELECT game_pk, pitcher_id, COALESCE(pitch_type, 'UNK') AS pitch_type, COUNT(*) AS n
          FROM pitch GROUP BY 1, 2, 3 ORDER BY n DESC)
    GROUP BY game_pk, pitcher_id
)
UPDATE box_score SET
    sc_pitches   = (SELECT sc_pitches   FROM agg WHERE agg.game_pk = box_score.game_pk AND agg.pitcher_id = box_score.player_id),
    sc_avg_velo  = (SELECT sc_avg_velo  FROM agg WHERE agg.game_pk = box_score.game_pk AND agg.pitcher_id = box_score.player_id),
    sc_max_velo  = (SELECT sc_max_velo  FROM agg WHERE agg.game_pk = box_score.game_pk AND agg.pitcher_id = box_score.player_id),
    sc_avg_spin  = (SELECT sc_avg_spin  FROM agg WHERE agg.game_pk = box_score.game_pk AND agg.pitcher_id = box_score.player_id),
    sc_whiffs    = (SELECT sc_whiffs    FROM agg WHERE agg.game_pk = box_score.game_pk AND agg.pitcher_id = box_score.player_id),
    sc_pitch_mix = (SELECT sc_pitch_mix FROM mix WHERE mix.game_pk = box_score.game_pk AND mix.pitcher_id = box_score.player_id)
WHERE pitches_thrown IS NOT NULL;
"""


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run(start, end, db_path, fresh):
    for d in (start, end):
        datetime.strptime(d, "%Y-%m-%d")  # fail fast on a bad date
    if start > end:
        sys.exit("start date must be on or before end date")
    if fresh and os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    run_at = datetime.now().isoformat(timespec="seconds")

    def log(step, n, note=None):
        conn.execute("INSERT INTO load_log VALUES (?,?,?,?,?,?)",
                     (run_at, start, end, step, n, note))
        print(f"  {step:<28} {n:>7}  {note or ''}")

    print(f"Loading {start} -> {end} into {db_path}")
    log("team", load_teams(conn))

    games = load_schedule(conn, start, end)
    played = [gm for gm in games if gm["is_played"]]
    log("schedule", len(games), f"{len(played)} completed, {len(games) - len(played)} not played")

    player_ids, n_box, n_pitch, issues = set(), 0, 0, []
    for i, gm in enumerate(played, 1):
        pk = gm["game_pk"]
        print(f"  game {i}/{len(played)}  {pk}", end="\r")
        try:
            n, ids = load_box_score(conn, pk)
            n_box += n
            player_ids |= ids
        except requests.RequestException as e:
            issues.append(f"{pk}: box score failed ({e})")
        try:
            n, ids, note = load_pitches(conn, pk)
            n_pitch += n
            player_ids |= ids
            if note:
                issues.append(f"{pk}: {note}")
        except (requests.RequestException, pd.errors.ParserError) as e:
            issues.append(f"{pk}: Statcast failed ({e})")
        conn.commit()
        time.sleep(0.2)  # be polite to Savant
    print(" " * 40, end="\r")
    log("box_score", n_box)
    log("pitch", n_pitch)

    log("player", load_players(conn, player_ids))
    conn.executescript(STATCAST_ROLLUP)
    log("box_score statcast rollup", conn.execute(
        "SELECT COUNT(*) FROM box_score WHERE sc_pitches IS NOT NULL").fetchone()[0])

    n, skipped, dupes = load_transactions(conn, start, end)
    log("transactions", n, f"removed {dupes} duplicate ids, skipped {skipped} without an id")

    for msg in issues:
        log("issue", 0, msg)
    conn.commit()
    conn.close()
    print(f"Done. {len(issues)} data issue(s) recorded in load_log.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load MLB data into SQLite.")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--db", default="mlb.db", help="SQLite file (default mlb.db)")
    ap.add_argument("--fresh", action="store_true", help="delete the database first")
    a = ap.parse_args()
    run(a.start, a.end, a.db, a.fresh)
