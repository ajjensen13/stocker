DROP VIEW IF EXISTS report."Candles52Wk";
CREATE VIEW report."Candles52Wk" AS
SELECT "Symbol",
       "Timestamp",
       "HighMax",
       "HighMin",
       "HighAvg",
       "HighCount",
       "LowMax",
       "LowMin",
       "LowAvg",
       "LowCount",
       "VolumeMax",
       "VolumeMin",
       "VolumeAvg",
       "VolumeCount"
FROM stage."Candles52Wk"