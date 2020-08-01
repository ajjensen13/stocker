CREATE SCHEMA IF NOT EXISTS stage;
ALTER SCHEMA stage OWNER TO stocker;
CREATE ROLE stocker_stage_ro;
GRANT stocker_stage_ro TO stocker;
ALTER DEFAULT PRIVILEGES IN SCHEMA stage GRANT SELECT ON TABLES TO stocker_stage_ro;
CREATE ROLE stocker_stage_rw;
GRANT stocker_stage_rw TO stocker;
ALTER DEFAULT PRIVILEGES IN SCHEMA stage GRANT ALL ON TABLES TO stocker_stage_rw;