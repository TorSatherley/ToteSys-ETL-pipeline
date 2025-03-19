import requests
import boto3
import os
import json
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pg8000.exceptions import DatabaseError
import logging
from datetime import datetime
from random import random, randint


from src.utils import return_datetime_string

from src.lambda_transform_utils import (
    read_s3_table_json,
    _return_df_dim_dates,
    _return_df_dim_design,
    _return_df_dim_location,
    populate_parquet_file,
    _return_df_dim_counterparty,
    _return_df_dim_staff,
    _return_df_dim_currency,
    _return_df_fact_sales_order,
    return_s3_key,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Function to transform the data landing in the ingestion bucket.
    JSON files are read from S3 and converted to pandas dataframes
    with extrapolated data to fit the fact and dimension tables of 
    the pre-existing star schema data warehouse.
    Dataframes are then converted to parquet format to be stored in
    the 'processed' S3 bucket, ready for loading.
    """
    try:
        # load_dotenv()
        # set up connection

        # read_s3_table_json(s3_client, s3_key, ingestion_bucket_name)

        # variables prep
        datetime_string = event["datetime_string"]
        s3_client = boto3.client("s3", region_name="eu-west-2")
        ingestion_bucket_name = os.environ.get("INGESTION_BUCKET")
        processed_bucket_name = os.environ.get("PROCESSED_BUCKET")
        responses = []

        output_data = {"quotes": []}

        # testing_backdoor
        if "testing_client" in event.keys() != None:
            s3_client = event["testing_client"]

        # read ingestion files
        df_totesys_sales_order = read_s3_table_json(
            s3_client,
            return_s3_key("sales_order", datetime_string),
            ingestion_bucket_name,
        )
        df_totesys_design = read_s3_table_json(
            s3_client, return_s3_key("design", datetime_string), ingestion_bucket_name
        )
        df_totesys_address = read_s3_table_json(
            s3_client, return_s3_key("address", datetime_string), ingestion_bucket_name
        )
        df_totesys_counterparty = read_s3_table_json(
            s3_client,
            return_s3_key("counterparty", datetime_string),
            ingestion_bucket_name,
        )
        df_totesys_staff = read_s3_table_json(
            s3_client, return_s3_key("staff", datetime_string), ingestion_bucket_name
        )
        df_totesys_department = read_s3_table_json(
            s3_client,
            return_s3_key("department", datetime_string),
            ingestion_bucket_name,
        )
        df_totesys_currency = read_s3_table_json(
            s3_client, return_s3_key("currency", datetime_string), ingestion_bucket_name
        )

        # produce and populate
        df_dim_dates = _return_df_dim_dates(df_totesys_sales_order)
        r = populate_parquet_file(
            s3_client, datetime_string, "dim_date", df_dim_dates, processed_bucket_name
        )
        responses += [r]

        df_dim_design = _return_df_dim_design(df_totesys_design)
        r = populate_parquet_file(
            s3_client,
            datetime_string,
            "dim_design",
            df_dim_design,
            processed_bucket_name,
        )
        responses += [r]

        df_dim_location = _return_df_dim_location(df_totesys_address)
        r = populate_parquet_file(
            s3_client,
            datetime_string,
            "dim_location",
            df_dim_location,
            processed_bucket_name,
        )
        responses += [r]

        df_dim_counterparty = _return_df_dim_counterparty(
            df_totesys_counterparty, df_totesys_address
        )
        r = populate_parquet_file(
            s3_client,
            datetime_string,
            "dim_counterparty",
            df_dim_counterparty,
            processed_bucket_name,
        )
        responses += [r]

        df_dim_staff = _return_df_dim_staff(df_totesys_staff, df_totesys_department)
        r = populate_parquet_file(
            s3_client, datetime_string, "dim_staff", df_dim_staff, processed_bucket_name
        )
        responses += [r]

        df_dim_currency = _return_df_dim_currency(df_totesys_currency)
        r = populate_parquet_file(
            s3_client,
            datetime_string,
            "dim_currency",
            df_dim_currency,
            processed_bucket_name,
        )
        responses += [r]

        df_fact_sales_order = _return_df_fact_sales_order(df_totesys_sales_order)
        r = populate_parquet_file(
            s3_client,
            datetime_string,
            "fact_sales_order",
            df_fact_sales_order,
            processed_bucket_name,
        )
        responses += [r]

        # response logic
        if all([200 == rn["ResponseMetadata"]["HTTPStatusCode"] for rn in responses]):
            logger.info("Wrote processed tables to S3 successfully")
            return {
                "statusCode": 200,
                "message": "Receipt processed successfully",
                "datetime_string": datetime_string,
                "responses_list": responses,
            }
        else:
            statusCodes = set(
                rn["ResponseMetadata"]["HTTPStatusCode"] for rn in responses
            )
            statusCodes.remove(200)
            logger.info(
                f"There was a problem. Quotes not written. Check Log, status codes include: {statusCodes}"
            )
            return {
                "statusCode": statusCodes,
                "message": "Receipt processed successfully",
                "datetime_string": datetime_string,
                "responses_list": responses,
            }
    except Exception as e:
        return str(e)


# Some code for live testing

# if __name__ == "__main__":
#
#    load_dotenv()
#    s3 = boto3.client("s3")
#
#
#
#    datetime_str = return_datetime_string()
#
#    jsonl_list = ["address","counterparty","currency","department","design","sales_order","staff"]
#    for jsonl_file in jsonl_list:
#        key = return_s3_key(jsonl_file, datetime_str)
#        with open(f"data/json_lines_s3_format/{jsonl_file}.jsonl", "rb") as file:
#            s3.put_object(Bucket=os.environ.get("INJESTION_BUCKET_NAME"), Key=key, Body=file.read())
#
#
#    event = {"datetime_string":datetime_str}
#    result = lambda_handler(event, "context")
