CREATE SCHEMA IF NOT EXISTS report;
ALTER SCHEMA report OWNER TO stocker;
CREATE ROLE stocker_report_ro;
GRANT stocker_report_ro TO stocker;
ALTER DEFAULT PRIVILEGES IN SCHEMA report GRANT SELECT ON TABLES TO stocker_report_ro;
CREATE ROLE stocker_report_rw;
GRANT stocker_report_rw TO stocker;
ALTER DEFAULT PRIVILEGES IN SCHEMA report GRANT ALL ON TABLES TO stocker_report_rw;