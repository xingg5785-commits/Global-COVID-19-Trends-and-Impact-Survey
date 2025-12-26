CREATE DATABASE ctis_db; 
USE ctis_db;

CREATE TABLE ctis_processed (
id INT AUTO_INCREMENT PRIMARY KEY,
`date` VARCHAR(100),
setting VARCHAR(255),
source VARCHAR(255),
indicator_abbr VARCHAR(100),
indicator_name TEXT,
dimension VARCHAR(255),
subgroup VARCHAR(255),
estimate DECIMAL(15, 5),
se DECIMAL(15, 5),
ci_lb DECIMAL(15, 5),
ci_ub DECIMAL(15, 5),
population DECIMAL(20, 5),
setting_average DECIMAL(15, 5),
iso3 VARCHAR(10),
favourable_indicator VARCHAR(255),
indicator_scale VARCHAR(50),
ordered_dimension INT,
subgroup_order INT,
reference_subgroup VARCHAR(255),
whoreg6 VARCHAR(50),
wbincome2023 VARCHAR(100),
`update` VARCHAR(100),
dataset_id VARCHAR(100)
);

ALTER TABLE ctis_processed
  MODIFY estimate DECIMAL(15,5) NULL,
  MODIFY se DECIMAL(15,5) NULL,
  MODIFY ci_lb DECIMAL(15,5) NULL,
  MODIFY ci_ub DECIMAL(15,5) NULL,
  MODIFY population DECIMAL(20,5) NULL,
  MODIFY setting_average DECIMAL(15,5) NULL,
  MODIFY ordered_dimension INT NULL,
  MODIFY subgroup_order INT NULL;

SELECT * FROM ctis_processed;

SELECT COUNT(*) FROM ctis_processed;

DROP TABLE ctis_processed;

Drop DATABASE ctis_db;


