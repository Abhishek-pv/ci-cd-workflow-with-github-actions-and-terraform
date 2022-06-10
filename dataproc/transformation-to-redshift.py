from functools import reduce
import os
import argparse
import logging
import datetime
from datetime import timedelta
import boto3
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, MapType


class TransformationPipeline:
    def __init__(
        self,
        redshift_cluster,
        redshift_port,
        redshift_user,
        redshift_database,
        redshift_region,
        redshift_schema,
        redshift_table,
        access_key_id,
        secret_access_key,
        start_date,
        end_date,
        bucket_name,
        bucket_region,
        type,
        start_hour,
        end_hour,
        spark,
        region,
    ):

        self.redshift_cluster = redshift_cluster
        self.redshift_port = redshift_port
        self.redshift_user = redshift_user
        self.redshift_database = redshift_database
        self.redshift_region = redshift_region
        self.redshift_schema = redshift_schema
        self.redshift_table = redshift_table
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        self.type = type
        self.start_date = start_date
        self.end_date = end_date
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.spark = spark
        self.region = region
        print(self.start_date)

    def data_path_generator(self):
        """Generate GCP cloud storage paths."""

        if (
            self.start_date is None
            and self.end_date is None
            and self.start_hour is None
            and self.end_hour is None
        ):

            current_time = datetime.datetime.now()
            year = current_time.year
            month = current_time.month
            day = current_time.day
            hour = current_time.hour
            previous_hour = hour - 1
            data_path = "gs://{bucket_name}/data/type={type}/region={bucket_region}/year={year}/month={month}/day={day}/hour={previous_hour}/*".format(
                bucket_name=self.bucket_name,
                type=self.type,
                bucket_region=self.bucket_region,
                year=year,
                month=month,
                day=day,
                previous_hour=previous_hour,
            )
            date = datetime.datetime(year, month, day, previous_hour)
            return [{date: data_path}]

        elif (
            self.start_date is not None
            and self.end_date is not None
            and self.start_hour is not None
            and self.end_hour is not None
        ):

            print(
                "Given start_date and end_date is {} and {}".format(
                    self.start_date, self.end_date
                )
            )
            date_format = "%Y-%m-%d"
            data_path_list = []

            start_date = datetime.datetime.strptime(self.start_date, date_format)
            end_date = datetime.datetime.strptime(self.end_date, date_format)

            if self.start_date == self.end_date:
                start_year = start_date.year
                start_month = start_date.month
                start_day = start_date.day

                for hour in range(int(self.start_hour), int(self.end_hour) + 1):
                    data_path = "gs://{bucket_name}/data/type={type}/region={bucket_region}/year={start_year}/month={start_month}/day={start_day}/hour={hour}/*".format(
                        bucket_name=self.bucket_name,
                        type=self.type,
                        bucket_region=self.bucket_region,
                        start_year=start_year,
                        start_month=start_month,
                        start_day=start_day,
                        hour=hour,
                    )
                    date = datetime.datetime(start_year, start_month, start_day, hour)
                    data_path_list.append({date: data_path})
                return data_path_list

            else:
                starting_hour = datetime.time(int(self.start_hour))
                ending_hour = datetime.time(int(self.end_hour))
                start_datetime = datetime.datetime.combine(start_date, starting_hour)
                end_datetime = datetime.datetime.combine(end_date, ending_hour)

                while start_datetime <= end_datetime:
                    year = start_datetime.year
                    month = start_datetime.month
                    day = start_datetime.day
                    hour = start_datetime.hour
                    data_path = "gs://{bucket_name}/data/type={type}/region={bucket_region}/year={year}/month={month}/day={day}/hour={hour}/*".format(
                        bucket_name=self.bucket_name,
                        type=self.type,
                        bucket_region=self.bucket_region,
                        year=year,
                        month=month,
                        day=day,
                        hour=hour,
                    )
                    data_path_list.append({start_datetime: data_path})
                    start_datetime = start_datetime + timedelta(hours=1)
                return data_path_list

        else:
            raise ValueError(
                "start_date, end_date, start_hour, end_hour must be all none or all not none!"
            )

    def read_data_from_gcs(self, data_path):

        print(
            "{}:Reading JSON files from CLoud Storage...".format(
                datetime.datetime.now()
            )
        )

        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("application_id", StringType()),
                StructField("grant_id", StringType()),
                StructField("sync_category", StringType()),
                StructField("object", StringType()),
                StructField("date", StringType()),
                StructField("steps", ArrayType(MapType(StringType(), StringType()))),
            ]
        )
        try:
            df = self.spark.read.json(data_path, schema)
            return df
        except:
            print("Google cloud storage path {} doesn't exist".format(data_path))
            return None

    def preprocessing_raw_data(self, raw_data, agg_date):

        print("Starting the processing of the raw data....")
        required_fields = [
            "id",
            "application_id",
            "grant_id",
            "sync_category",
            "object",
            "date",
            "steps",
        ]
        raw_data = raw_data[required_fields]

        raw_data = raw_data.withColumn("agg_date", lit(agg_date)).withColumn(
            "region", lit(self.region)
        )

        json_converted_df = raw_data.select(
            "id",
            "application_id",
            "grant_id",
            "sync_category",
            "object",
            "agg_date",
            "region",
            explode("steps").alias("steps"),
        )
        processed_df = json_converted_df.select(
            "id",
            "application_id",
            "grant_id",
            "sync_category",
            "object",
            "agg_date",
            "region",
            json_converted_df.steps["id"].alias("step_id"),
            json_converted_df.steps["type"].alias("step_type"),
            json_converted_df.steps["status"].alias("step_status"),
            json_converted_df.steps["tier"].alias("step_tier"),
        )
        processed_df.show()
        print("Pre-processing is complete....")
        return processed_df

    def buisness_logic(self):

        data_path_list = self.data_path_generator()
        for dic in data_path_list:
            for agg_date, data_path in dic.items():
                print("Reading the data from the path {}".format(data_path))
                raw_data = self.read_data_from_gcs(data_path)
                if raw_data == None and len(data_path_list) > 1:
                    continue
                elif len(data_path_list) == 1 and raw_data == None:
                    print("No data to be loaded")
                    break
                processed_df = self.preprocessing_raw_data(raw_data, agg_date)
                processed_df.show(10, truncate=False)
                processed_df.createOrReplaceTempView("processed_table")

                final_df = spark.sql(
                    """ 
                        select 
                        region, 
                        agg_date as date,
                        application_id,
                        grant_id,
                        sync_category,
                        object as object_type,
                        step_type,
                        step_tier,
                        step_status,
                        count (distinct concat(id,step_id)) as count_transformations
                        from processed_table 
                        group by region, date, application_id, grant_id, sync_category, object_type, step_type, step_tier,step_status
                    """
                )

                client = boto3.client(
                    "redshift",
                    aws_access_key_id=self.access_key_id,
                    aws_secret_access_key=self.secret_access_key,
                    region_name=self.redshift_region,
                )

                url = "jdbc:postgresql://{cluster}.ctbvat5gr6rr.{region}.redshift.amazonaws.com:{port}/{db}".format(
                    cluster=self.redshift_cluster,
                    region=self.redshift_region,
                    port=self.redshift_port,
                    db=self.redshift_database,
                )

                table = "{}.{}".format(self.redshift_schema, self.redshift_table)

                credentials = client.get_cluster_credentials(
                    DbUser=self.redshift_user,
                    DbName=self.redshift_database,
                    ClusterIdentifier=self.redshift_cluster,
                    DurationSeconds=3600,
                    AutoCreate=True,
                )

                properties = {
                    "user": credentials["DbUser"],
                    "password": credentials["DbPassword"],
                    "driver": "org.postgresql.Driver",
                }

                final_df.coalesce(10).write.jdbc(
                    url=url, table=table, mode="append", properties=properties
                )


if __name__ == "__main__":

    logging.getLogger().setLevel(logging.ERROR)
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket_name", help="data_streams_bucket,..")
    parser.add_argument("bucket_region", help="us-west-1, us-west-2,...")
    parser.add_argument("type", help="inflated,transformed,..")
    parser.add_argument("start_date", help="2021-01-01,..")
    parser.add_argument("end_date", help="2021-01-02,...")
    parser.add_argument("start_hour", help="0,1,2,3,..23")
    parser.add_argument("end_hour", help="0,1,2,3,..23")
    parser.add_argument("region", help="us-west-1, us-west-2,...")
    args, pipeline_args = parser.parse_known_args()

    # create spark session
    spark = SparkSession.builder.appName("Transformed topic").getOrCreate()

    # Redshift details
    redshift_cluster = "fivetran-poc"
    redshift_port = 5439
    redshift_user = "dataengineer"
    redshift_database = "dev"
    redshift_region = "us-east-2"
    redshift_schema = "nylas_gma"
    redshift_table = "transformation"
    access_key_id = os.environ["AWS_ACCESS_KEY"]
    secret_access_key = os.environ["AWS_SECRET_KEY"]

    pipeline = TransformationPipeline(
        redshift_cluster,
        redshift_port,
        redshift_user,
        redshift_database,
        redshift_region,
        redshift_schema,
        redshift_table,
        access_key_id,
        secret_access_key,
        args.start_date,
        args.end_date,
        args.bucket_name,
        args.bucket_region,
        args.type,
        args.start_hour,
        args.end_hour,
        spark,
        args.region,
    )
    pipeline.buisness_logic()

    print("Load is complete...")
