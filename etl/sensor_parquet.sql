---------------------------------------------------------------
-- SQL for creating external table for Amazon Redshift Spectrum
-- enables querying parquet files in a data lake on S3
---------------------------------------------------------------

CREATE EXTERNAL TABLE spectrum.sensor_avg
(
    sensor_number bigint,
    reading_date timestamp,
    avg_temp float,
    avg_humidity float,
    avg_light_level float
)
STORED AS PARQUET
location 's3://real-time-sensors/data_lake';
