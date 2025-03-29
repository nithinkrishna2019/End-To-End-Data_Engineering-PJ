import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DataType,BooleanType,DecimalType,TimestampType
from pyspark.sql.functions import col,to_timestamp, date_format, year, month, dayofmonth,coalesce
from datetime import datetime,timezone, timedelta

import boto3
import os


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#taking backup of my source file
IST_OFFSET = timedelta(hours=5, minutes=30)
timestamp_backup = (datetime.now(timezone.utc) + IST_OFFSET).strftime("%Y%m%d_%H%M%S")


bucket_name = "aws-glue-s3-bucket"
source_prefix = "End-to-End-PJ/source-Data-For-ETL/"

destination_prefix = f"End-to-End-PJ/source-backup-before-ETL/{timestamp_backup}/"


def backup_source_file():
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
    if "Contents" in response:
        file_names = []  # Empty list to store valid JSON file names

        for obj in response["Contents"]:
            file_key = obj["Key"]  # Get the file path from S3
            
            # Ignore the folder itself and only select .json files
            if file_key != source_prefix and file_key.endswith(".json"):
                file_names.append(file_key)

        for i in file_names:
            file_name_final = os.path.basename(i)  # Extract only filename
            destination_key = destination_prefix + file_name_final  # New S3 path

            # Copy file to the destination
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': i},
                Key=destination_key
            )


#read data from s3 path
s3_path = "s3a://aws-glue-s3-bucket/End-to-End-PJ/source-Data-For-ETL/*.json"

#performing ETL
def weather_data_etl(s3_path):
    df = spark.read.option("multiline", "true").json(s3_path)
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    weather_data=df.select(
    col("ID"),
    col("city"),
    col("country"),
    col("timestamp"),
    year("timestamp").alias("year"),
    month("timestamp").alias("month"),
    dayofmonth("timestamp").alias("day"),
    date_format(col("timestamp"), "HH:mm").alias("time"),
    col("weather.temperature_celsius").alias("temperature_celsius"),
    col("weather.humidity_percent").alias("humidity_percent"),
    col("weather.pressure_hpa").alias("pressure_hpa"),
    col("weather.wind_speed_kph").alias("wind_speed_kph"),
    col("weather.wind_direction").alias("wind_direction"),
    col("weather.conditions").alias("conditions")
    )
    weather_data_transformed=weather_data

    return weather_data_transformed

timestamp_target = (datetime.now(timezone.utc) + IST_OFFSET).strftime("%Y%m%d_%H%M%S")

s3_path_target = f"s3a://aws-glue-s3-bucket/End-to-End-PJ/transformed_data/{timestamp_target}/"

#writing transformed data to S3
def transformed_data_to_s3():
    weather_data_transformed.coalesce(1).write.mode("overwrite").csv(s3_path_target, header=True)


def delete_source_file():
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
    if "Contents" in response:
        file_names = []  # Empty list to store valid JSON file names

        for obj in response["Contents"]:
            file_key = obj["Key"]  # Get the file path from S3
            
            # Ignore the folder itself and only select .json files
            if file_key != source_prefix and file_key.endswith(".json"):
                file_names.append(file_key)

        for i in file_names:
            s3_client.delete_object(Bucket=bucket_name, Key=i)
            


backup_source_file() #1
weather_data_transformed=weather_data_etl(s3_path) #2
transformed_data_to_s3() #3
delete_source_file() #4


job.commit()