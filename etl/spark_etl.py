from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col, current_timestamp, to_date
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import TimestampType, DateType


"""
    Spark job on Amazon EMR 
    Writes sensor reading data to parquet files on S3-based data lake
"""

def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_sensor_data(spark, input_data, output_data):
    """
        Process sensor data files.
        Compute mean temperature, humidity and light readings for each sensor and reading date.
        Write data to parquet files.
    """
    
    # get filepath to sensor readings data    
    sensor_data = input_data + 'redshift_processed/*/*/*/*.json'

    # read sensor data
    print("----- reading sensor data files -----")
    df = spark.read.json(sensor_data)

    # create reading date column from epoch timestamp column
    df = df.withColumn("reading_date", to_date(col("timestamp").cast(dataType=TimestampType())))

    # group by sensor number and reading date
    # create columns for mean values for temp, humidity and light per sensor and date
    df = df.groupBy("sensor_number", "reading_date") \
    .agg(mean("ambient_temperature").alias("avg_temp"), mean("humidity").alias("avg_humidity"), mean("photosensor").alias("avg_light_level")) \
    .orderBy("sensor_number", "reading_date")  

    # create columns for partioning parquet files
    df = df.withColumn("sensor_no_part", df["sensor_number"])

    # write daily sensor metrics to parquet files
    # partition files by sensor number, year, month and day
    print("----- writing sensor table parquet files -----")
    fields = ["sensor_number", "reading_date", "avg_temp", "avg_humidity", "avg_light_level","sensor_no_part"]
    sensor_table = df.selectExpr(fields).dropDuplicates()
    sensor_table.write.mode('overwrite').partitionBy("sensor_no_part").parquet(output_data)


def main():
    spark = create_spark_session()
    input_data = "s3a://real-time-sensors/"
    output_data = "s3a://real-time-sensors/data_lake/"
    
    process_sensor_data(spark, input_data, output_data)    


if __name__ == "__main__":
    main()    