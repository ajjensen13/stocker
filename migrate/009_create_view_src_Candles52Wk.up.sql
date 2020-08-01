CREATE VIEW src."Candles52Wk" AS
SELECT s."Symbol",
       s."Timestamp",
       MAX(lag."High")     "HighMax",
       MIN(lag."High")     "HighMin",
       AVG(lag."High")     "HighAvg",
       COUNT(lag."High")   "HighCount",
       MAX(lag."Low")      "LowMax",
       MIN(lag."Low")      "LowMin",
       AVG(lag."Low")      "LowAvg",
       COUNT(lag."Low")    "LowCount",
       MAX(lag."Volume")   "VolumeMax",
       MIN(lag."Volume")   "VolumeMin",
       AVG(lag."Volume")   "VolumeAvg",
       COUNT(lag."Volume") "VolumeCount"
FROM src."Candles" s
         JOIN src."Candles" lag
              ON s."Symbol" = lag."Symbol"
                  AND date_trunc('day', lag."Timestamp")
                     BETWEEN date_trunc('day', s."Timestamp" - INTERVAL '52 week')
                     AND date_trunc('day', s."Timestamp")
GROUP BY s."Symbol",
         s."Timestamp"