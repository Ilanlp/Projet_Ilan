USE DATABASE JOB_MARKET;
USE SCHEMA RAW;



CREATE TABLE IF NOT EXISTS RAW_OFFRE (
  id_offre NUMBER AUTOINCREMENT,
  id_local VARCHAR(14),
  title VARCHAR(1000),
  description VARCHAR(16777216),
  date_created TIMESTAMP_LTZ,
  location VARCHAR(10000),
  latitude VARCHAR(200),
  longitude VARCHAR(200),
  company VARCHAR(10000),
  source_url VARCHAR(16777216),
  contract_type VARCHAR(20),
  seniorite_type VARCHAR(100),
  source VARCHAR(100),
  date_extraction TIMESTAMP_LTZ
);