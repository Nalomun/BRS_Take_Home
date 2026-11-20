-- Example queries showing the data loaded correctly.
-- Run all of them with:  sqlite3 -header -column mlb.db < queries.sql

-- 1. Row counts per table.
SELECT 'team' AS tbl, COUNT(*) AS n FROM team
UNION ALL SELECT 'player', COUNT(*) FROM player
UNION ALL SELECT 'schedule', COUNT(*) FROM schedule
UNION ALL SELECT 'box_score', COUNT(*) FROM box_score
UNION ALL SELECT 'pitch', COUNT(*) FROM pitch
UNION ALL SELECT 'transactions', COUNT(*) FROM transactions;

-- 2. Schedule with team names and scores (unplayed games show NULL scores).
SELECT s.official_date, a.abbreviation AS away, s.away_score,
       h.abbreviation AS home, s.home_score, s.detailed_state
FROM schedule s
LEFT JOIN team a ON a.team_id = s.away_team_id
LEFT JOIN team h ON h.team_id = s.home_team_id
ORDER BY s.official_date, s.game_pk
LIMIT 15;

-- 3. Score check: box score runs should add up to the schedule's final score.
SELECT s.game_pk, s.home_score, SUM(b.r) AS home_runs_from_box,
       CASE WHEN s.home_score = SUM(b.r) THEN 'ok' ELSE 'MISMATCH' END AS check_result
FROM schedule s
JOIN box_score b ON b.game_pk = s.game_pk AND b.team_id = s.home_team_id
WHERE s.is_played = 1
GROUP BY s.game_pk
ORDER BY check_result DESC
LIMIT 10;

-- 4. Pitch-count reconciliation: MLB box score count vs Statcast rows.
SELECT COUNT(*) AS pitcher_games,
       SUM(pitches_thrown = sc_pitches) AS exact_match,
       SUM(sc_pitches IS NULL) AS no_statcast,
       SUM(pitches_thrown <> sc_pitches) AS mismatch
FROM box_score
WHERE pitches_thrown IS NOT NULL;

-- 5. Hardest-throwing starters, with Statcast metrics from BOX_SCORE.
SELECT p.full_name, t.abbreviation AS team, b.game_pk, b.ip, b.p_so,
       b.pitches_thrown, b.sc_avg_velo, b.sc_max_velo, b.sc_whiffs, b.sc_pitch_mix
FROM box_score b
JOIN player p ON p.player_id = b.player_id
LEFT JOIN team t ON t.team_id = b.team_id
WHERE b.outs_recorded >= 15
ORDER BY b.sc_avg_velo DESC
LIMIT 10;

-- 6. Pitch mix and average velocity by pitch type across the whole range.
SELECT pitch_type, pitch_name, COUNT(*) AS pitches,
       ROUND(AVG(release_speed), 1) AS avg_velo,
       ROUND(AVG(release_spin_rate)) AS avg_spin
FROM pitch
GROUP BY pitch_type, pitch_name
ORDER BY pitches DESC;

-- 7. Red Sox transactions in the period, with team names.
SELECT transaction_date, type_desc, player_name, from_team_name, to_team_name
FROM transactions
WHERE from_team_id = 111 OR to_team_id = 111
ORDER BY transaction_date;

-- 8. Data issues the pipeline recorded during the load.
SELECT step, note FROM load_log WHERE step = 'issue';
