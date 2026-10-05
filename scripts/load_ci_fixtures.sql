CREATE SCHEMA IF NOT EXISTS oecd_dac_crs;
CREATE OR REPLACE TABLE oecd_dac_crs.raw_dac_crs AS
    SELECT * FROM read_parquet('tests/oecd/dac_crs/fixtures/data.parquet');
CREATE OR REPLACE TABLE oecd_dac_crs.raw_dac_crs_metadata AS
    SELECT
        TIMESTAMPTZ '2026-08-18 09:14:22+00' AS downloaded_at,
        DATE '2026-08-03' AS source_release_date;

CREATE SCHEMA IF NOT EXISTS world_bank_ids;
CREATE OR REPLACE TABLE world_bank_ids.raw_ids AS
    SELECT * FROM read_parquet('tests/world_bank/ids/fixtures/data.parquet');
CREATE OR REPLACE TABLE world_bank_ids.raw_ids_metadata AS
    SELECT
        TIMESTAMPTZ '2026-09-29 09:14:22+00' AS downloaded_at,
        DATE '2025-12-03' AS source_release_date;

CREATE SCHEMA IF NOT EXISTS dfo_ontology;
CREATE OR REPLACE TABLE dfo_ontology.raw_code_mapping AS
    SELECT * FROM read_csv('tests/dfo/ontology/fixtures/codes.csv',
                           header = true, all_varchar = true);

CREATE SCHEMA IF NOT EXISTS one_imputed_multilateral_sectors;
CREATE OR REPLACE TABLE one_imputed_multilateral_sectors.raw_imputed_multilateral_sectors AS
    SELECT * FROM read_parquet('tests/one/imputed_multilateral_sectors/fixtures/data.parquet');
CREATE OR REPLACE TABLE one_imputed_multilateral_sectors.raw_imputed_multilateral_sectors_metadata AS
    SELECT
        TIMESTAMPTZ '2026-08-18 09:14:22+00' AS downloaded_at,
        DATE '2026-08-03' AS crs_release_date,
        DATE '2025-12-18' AS multisystem_release_date;

CREATE SCHEMA IF NOT EXISTS imf_weo;
CREATE OR REPLACE TABLE imf_weo.raw_weo AS
    SELECT * FROM read_parquet('tests/imf/weo/fixtures/data.parquet');
-- Germany and France give the conversion tables' euro agreement check a member to compare.
INSERT INTO imf_weo.raw_weo
    SELECT * FROM read_parquet('tests/imf/weo/fixtures/conversion_tables_euro_members.parquet');
CREATE OR REPLACE TABLE imf_weo.raw_weo_metadata AS
    SELECT
        TIMESTAMPTZ '2026-09-11 09:14:22+00' AS downloaded_at,
        DATE '2026-04-14' AS source_release_date;

CREATE SCHEMA IF NOT EXISTS oecd_dac1;
CREATE OR REPLACE TABLE oecd_dac1.raw_dac1 AS
    SELECT * FROM read_parquet('tests/oecd/dac1/fixtures/raw_dac1.parquet');
CREATE OR REPLACE TABLE oecd_dac1.raw_dac1_metadata AS
    SELECT
        TIMESTAMPTZ '2026-09-15 09:14:22+00' AS downloaded_at,
        DATE '2026-07-07' AS source_release_date;
