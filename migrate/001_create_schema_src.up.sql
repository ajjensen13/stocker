CREATE SCHEMA IF NOT EXISTS src;
ALTER SCHEMA src OWNER TO stocker;
CREATE ROLE stocker_src_ro;
GRANT stocker_src_ro TO stocker;
ALTER DEFAULT PRIVILEGES IN SCHEMA src GRANT SELECT ON TABLES TO stocker_src_ro;
CREATE ROLE stocker_src_rw;
GRANT stocker_src_rw TO stocker;
ALTER DEFAULT PRIVILEGES IN SCHEMA src GRANT ALL ON TABLES TO stocker_src_rw;