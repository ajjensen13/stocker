DROP VIEW IF EXISTS src."Candles52Wk";
CREATE VIEW src."Candles52Wk" AS
SELECT s."Symbol",
       s."Timestamp",
       MAX(lag."High")     "52WkHighMax",
       MIN(lag."High")     "52WkHighMin",
       AVG(lag."High")     "52WkHighAvg",
       COUNT(lag."High")   "52WkHighCount",
       MAX(lag."Low")      "52WkLowMax",
       MIN(lag."Low")      "52WkLowMin",
       AVG(lag."Low")      "52WkLowAvg",
       COUNT(lag."Low")    "52WkLowCount",
       MAX(lag."Volume")   "52WkVolumeMax",
       MIN(lag."Volume")   "52WkVolumeMin",
       AVG(lag."Volume")   "52WkVolumeAvg",
       COUNT(lag."Volume") "52WkVolumeCount"
FROM src."Candles" s
         JOIN src."Candles" lag
              ON s."Symbol" = lag."Symbol"
                  AND date_trunc('day', lag."Timestamp")
                     BETWEEN date_trunc('day', s."Timestamp" - INTERVAL '52 week')
                     AND date_trunc('day', s."Timestamp")
GROUP BY s."Symbol",
         s."Timestamp"