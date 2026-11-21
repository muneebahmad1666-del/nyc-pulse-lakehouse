-- Run ONCE per environment, by someone allowed to create catalogs (you, in your own workspace).
-- Replace BUCKET with your bucket name.
--
-- First, outside this file: give Databricks access to the bucket. In Catalog Explorer, create a
-- storage credential (an AWS role Databricks can assume) and an external location that covers
-- s3://BUCKET/ (or at least raw/, dev/ and prod/). Without it, every statement below fails.
--
-- What each folder is for:
--   dev/managed/  the catalog's managed location (Unity Catalog's own storage)
--   dev/tables/   our external Delta tables (bronze, silver, gold)   <- not inside managed/
--   dev/state/    run logs and anything the pipeline needs to remember

CREATE CATALOG IF NOT EXISTS dev  MANAGED LOCATION 's3://BUCKET/dev/managed';
CREATE CATALOG IF NOT EXISTS prod MANAGED LOCATION 's3://BUCKET/prod/managed';

CREATE SCHEMA IF NOT EXISTS dev.bronze;
CREATE SCHEMA IF NOT EXISTS dev.silver;
CREATE SCHEMA IF NOT EXISTS dev.gold;

CREATE SCHEMA IF NOT EXISTS prod.bronze;
CREATE SCHEMA IF NOT EXISTS prod.silver;
CREATE SCHEMA IF NOT EXISTS prod.gold;

-- Quick test that raw/ is readable (should list the folders the extractor wrote):
-- LIST 's3://BUCKET/raw/';