CREATE TABLE IF NOT EXISTS
  `voterfile.pres24_vote_totals` AS
SELECT
  `County`,
  `Precinct`,
  `Total Votes`,
  `Choice`,
  `Choice Party`
FROM
  `voterfile.results_pct_20241105`
WHERE
  `Contest Name` = 'US PRESIDENT'
  AND `Choice Party` IN ('REP',
    'DEM')