import boto3
import io
import os
import psycopg2
import pandas as pd
import json
import time
from src.utils import get_secret
from src.lambda_transform_utils import return_s3_key


def lambda_handler(event, context):
    """
    retrieves parquet files from the 'processed' S3 bucket, and then loads them
    into the appropriate table in the data warehouse.
    """
    try:
        # Retrieve credentials
        sm_client = boto3.client(service_name="secretsmanager", region_name="eu-west-2")
        if not "SECRET_NAME" in event.keys():
            secret_name = os.environ.get("SECRET_NAME")
        else:
            secret_name = event["SECRET_NAME"]
        db_credentials = get_secret(sm_client, secret_name)
        dw_cleanup(db_credentials)

        # Connection
        print("Loading started...")
        start_time = time.time()
        conn = load_connection_psycopg2(db_credentials)
        cursor = conn.cursor()

        bucket_name = "totesys-processed-zone-fenor"
        s3_client = boto3.client("s3", region_name="eu-west-2")

        list_of_tables = [
            "dim_date",
            "dim_design",
            "dim_location",
            "dim_counterparty",
            "dim_staff",
            "dim_currency",
            "fact_sales_order",
        ]

        # Insert statement
        for file in list_of_tables:

            # Read parquet file
            s3_key = return_s3_key(file, event["datetime_string"], extension=".parquet")
            s3_response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            parquet_data = s3_response["Body"].read()
            df = pd.read_parquet(io.BytesIO(parquet_data))
            df = df.reset_index()

            # Build Insert query
            df_columns = df.columns.tolist()
            df_columns = ", ".join(df_columns)
            df_placeholders = ", ".join(["%s"] * len(df.columns))

            insert_query = f"""
            INSERT INTO {file} 
            ({df_columns})
            VALUES 
            ({df_placeholders})
            """

            for _, row in df.iterrows():
                row_dict = row.to_dict()
                cursor.execute(insert_query, tuple(row_dict.values()))
            conn.commit()

        cursor.close()
        conn.close()
        end_time = time.time()
        execution_time = end_time - start_time
        return {"message": "Successfully uploaded to data warehouse"}
    except Exception as e:
        return {"message": f"Error: {e}"}


def load_connection_psycopg2(db_credentials):
    ''' use psychopg to connect to the data warehouse. '''
    try:
        conn = psycopg2.connect(
            dbname=db_credentials["database"],
            user=db_credentials["user"],
            password=db_credentials["password"],
            host=db_credentials["host"],
            port=5432,
        )
        return conn
    except Exception as e:
        return {
            "message": str(e)
            + ", this is likely a secret credentials issue, either they are wrong or the connection to the aws manager couldn't be made"
        }


def dw_cleanup(db_credentials):
    '''
    reset the datawarehouse ready for the data to be reuploaded.
    Future improvements for this ETL pipeline would include only
    collecting new and updated data from the ToteSys database and
    refactoring lambda transform to be able to deal with this data 
    and finally refactoring lambda load so that it only needs to
    update the data warehouse with new or updated data rather than
    totally re-seeding it each time.
    '''
    conn = load_connection_psycopg2(db_credentials)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fact_sales_order")
    cursor.execute("DELETE FROM dim_counterparty")
    cursor.execute("DELETE FROM dim_currency")
    cursor.execute("DELETE FROM dim_date")
    cursor.execute("DELETE FROM dim_design")
    cursor.execute("DELETE FROM dim_location")
    cursor.execute("DELETE FROM dim_staff")
    conn.commit()
    return {"message": "Date Warehouse restored to default state."}
