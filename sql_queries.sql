-- Creating minio/hive schema and table
CREATE SCHEMA minio.you_schema WITH (location = 's3a://your_bucket_in_s3/');

CREATE TABLE minio.your_schema.your_table (
  	-- enter your column names with varchar datatype
    )
WITH (
  skip_header_line_count = 1,
  external_location = 's3a://datalake/',
  format = 'CSV'
);

-- Creating iceberg schema and table
CREATE SCHEMA iceberg.your_iceberg_schema WITH (location = 's3a://your_bucket_in_s3/');

CREATE TABLE iceberg.your_iceberg_schema.your_iceberg_table
WITH (
  location = 's3a://datalake/',
  format = 'PARQUET'
) as 
select * from minio.your_schema.your_table;

-- Viewing the data
select * from minio.you_schema.your_table;
select * from iceberg.your_iceberg_schema.your_iceberg_table;
