USE DATABASE JOB_MARKET;
USE SCHEMA RAW;



CREATE TABLE IF NOT EXISTS RAW_OFFRE (
  id_offre NUMBER AUTOINCREMENT,
  id_local VARCHAR(14),
  source VARCHAR(10000),
  title VARCHAR(1000),
  description VARCHAR(16777216),
  company_name VARCHAR(10000),
  location_name VARCHAR(10000),
  latitude VARCHAR(200),
  longitude VARCHAR(200),
  date_created TIMESTAMP_LTZ,
  date_updated TIMESTAMP_LTZ,
  contract_type VARCHAR(20),
  contract_duration VARCHAR(50),
  working_hours VARCHAR(14),
  salary_min INT,
  salary_max INT,
  salary_currency VARCHAR(10),
  salary_period VARCHAR(8),
  experience_required VARCHAR(30),
  category VARCHAR(100),
  sector VARCHAR(150),
  application_url VARCHAR(16777216),
  source_url VARCHAR(16777216),
  skills VARCHAR(16777216),
  remote_work VARCHAR(10),
  is_handicap_accessible BOOLEAN,
  code_rome VARCHAR(5),
  langues VARCHAR(100),
  date_extraction TIMESTAMP_LTZ
);