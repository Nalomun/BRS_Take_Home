# MLB Data Pipeline

Pulls MLB data for a date range from the MLB Stats API and Baseball Savant (Statcast), cleans it up, and loads it into a SQLite database. Six main tables: `team`, `player`, `schedule`, `box_score`, `pitch`, `transactions`.

## Setup

Python 3.9+ should be fine (I ran it on 3.13).

```bash
pip install -r requirements.txt
```

## Running it

```bash
python3 pipeline.py --start 2026-08-01 --end 2026-08-03 --fresh
```

- `--start` / `--end`: the date range, inclusive (YYYY-MM-DD). Required.
- `--db`: where to write the database. Defaults to `mlb.db`.
- `--fresh`: deletes the database first. Without it, loads just upsert, so re-running a range or tacking on a new one won't create duplicates.

The `mlb.db` in this folder came from the command above. To sanity check it:

```bash
sqlite3 -header -column mlb.db < queries.sql
```

## How it works

Everything is in `pipeline.py`, roughly one function per table. The order is:

1. Teams from `/teams?sportId=1`
2. Schedule for the date range, with each game flagged as played or not
3. For every game that was actually played, the box score from `/game/{gamePk}/boxscore` and the Statcast CSV from Savant
4. Players from `/people?personIds=...` (batched 100 at a time) for everyone who showed up in a box score or the pitch data
5. A SQL rollup that aggregates the pitch table per pitcher per game and writes it onto that pitcher's box score row
6. Transactions for the same date range

Box scores and Statcast CSVs for finished games get cached in `raw/` since that data doesn't change, which makes re-runs a lot faster. Schedule, teams and transactions are always pulled fresh. Each run also writes row counts and any data problems to a `load_log` table, mostly so I could see what happened without scrolling back through terminal output.

## Table design

| Table | One row per | Key |
|---|---|---|
| `team` | team | `team_id` |
| `player` | player | `player_id` |
| `schedule` | game | `game_pk` |
| `box_score` | player per game | `game_pk, player_id` |
| `pitch` | pitch | `game_pk, at_bat_number, pitch_number` |
| `transactions` | transaction | `transaction_id` |

Some notes on the choices:

I used the source IDs as keys instead of making my own. `gamePk`, team IDs and player IDs are stable, and the player ID in the Stats API is the same MLBAM ID Statcast uses for `pitcher` and `batter`. That shared ID is basically what makes the whole thing join together, so it seemed better to keep it than to add surrogate keys on top.

`box_score` has batting and pitching side by side in one wide table, with NULLs where a line doesn't apply (a shortstop has no pitching line). Two tables would arguably be cleaner, but the prompt asked for six outputs, so I kept it as one. Splitting it would be the first thing I'd change if this grew.

The Statcast columns on `box_score` (`sc_pitches`, `sc_avg_velo`, `sc_max_velo`, `sc_avg_spin`, `sc_whiffs`, `sc_pitch_mix`) are calculated from `pitch` with SQL instead of being copied in separately. That way the pitch table is the one source for pitch data, and the rollup can always just be rebuilt. `sc_pitch_mix` is a small JSON string like `{"FF": 54, "SL": 20}`.

`pitch` doesn't have every Statcast field, just about 35 that seemed useful: game context (count, inning, handedness), the pitch itself (type, velo, spin, movement, location) and the result (description, event, exit velo, launch angle, xBA, run value).

`transactions` stores team and player names along with the IDs, since a lot of transactions involve minor league teams that aren't in `team`.

Foreign keys are declared mainly to document the relationships. SQLite doesn't enforce them unless you turn it on.

## Messy data and how I handled it

- Postponed games come back from the API with `abstractGameState = "Final"`. A game only counts as played if its detailed state is Final, Game Over or Completed Early. Box scores and Statcast only get pulled for those, and unplayed games stay in `schedule` with NULL scores.
- A suspended game can show up on two dates with the same `gamePk`. I keep one row, preferring the completed one.
- Box scores list bench guys who never got in the game. Anyone with empty batting and pitching stats gets skipped.
- Innings pitched comes as text like `"5.2"`, which means 5 and 2/3, not 5.2. I kept the text in `ip` and added `outs_recorded` (17 in that case) so it's actually usable in math.
- The Statcast CSV can start with a byte-order mark, has blank fields, and could come back empty or as an HTML error page. The BOM gets stripped, blanks become NULL, and an empty or HTML response gets logged and that game is skipped.
- Duplicate pitch rows: the primary key keeps one, and the count goes in the log.
- The transactions endpoint repeats IDs. For this range it returned 387 rows but only 269 unique transactions, so I dedupe on `transaction_id` and log how many got dropped. Rows with no ID at all are skipped and counted, and a missing player or team is just NULL.
- Network errors and rate limits get retried with backoff. If one game fails, it's logged and the run keeps going.

## What's in the included database

For 2026-08-01 to 2026-08-03: 38 games, 30 teams, 655 players, 1,102 box score rows, 11,115 pitches, 269 transactions.

Two checks I ran (they're in `queries.sql`):

- Runs added up from `box_score` match the final score in `schedule` for each game (query 3).
- Pitch counts mostly line up. Of 317 pitcher lines, 301 match MLB's official count exactly, 15 have 1 to 5 more Statcast rows than MLB counted, and 1 has no Statcast match. I kept both numbers instead of forcing them to agree. I didn't have time to dig into why, but my guess is Statcast logs some non-pitch events (pitch clock automatic balls, maybe) as rows. That's what I'd look at next.

## Assumptions

- MLB regular season only (`sportId=1`). If you load a range with spring training games against non-MLB teams, those games would land in `schedule` with no matching `team` row.
- `player` covers everyone who appeared in a loaded game. Players who only show up in transactions (mostly minor leaguers) are in `transactions` by ID and name but aren't added to `player`, otherwise the table would fill up with thousands of guys who don't appear anywhere else.
- A whiff means a pitch with description `swinging_strike` or `swinging_strike_blocked`.
- Transactions use the same date range as the schedule.

## Files

- `pipeline.py`: the pipeline
- `queries.sql`: example and check queries
- `requirements.txt`
- `mlb.db`: the database it produced
- `raw/`: cached API responses, safe to delete

---

Built with AI assistance; all design decisions reviewed and validated against live data.
