import json
from datetime import datetime, date
from botocore.exceptions import ClientError, NoCredentialsError
import pandas as pd
from pg8000.native import Connection
from pg8000.exceptions import DatabaseError


def get_secret(sm_client, secret_name):
    """Retrieves database secrets from AWS Secrets Manager."""
    if not secret_name:
        raise ValueError("SECRET_NAME environment variable is not set.")
    try:
        get_secret_value_response = sm_client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        secrets = json.loads(secret)
        return secrets
    except (
        ClientError,
        NoCredentialsError,
        ValueError,
        json.JSONDecodeError,
        KeyError,
        Exception,
    ) as e:
        print(f"Secret retrieval error: {e}")
        raise e


def create_conn(db_credentials):
    """Establishes a connection to the database using secrets."""
    try:
        db_connection = Connection(
            database=db_credentials["dbname"],
            user=db_credentials["username"],
            password=db_credentials["password"],
            host=db_credentials["host"],
        )
        return db_connection
    except (DatabaseError, TypeError, KeyError, Exception) as e:
        print(f"Database connection error: {e}")
        raise e


def close_db(conn):
    """Closes the database connection."""
    try:
        conn.close()
    except (AttributeError, Exception) as e:
        print(f"Error closing database connection: {e}")
        raise e


def get_rows_and_columns_from_table(conn, table):
    """Fetches rows and column names from a database table."""
    try:
        columns_query = conn.run(
            f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}'"
        )
        columns = [column[0] for column in columns_query]
        rows = conn.run(f"SELECT * FROM {table}")
        return rows, columns
    except Exception as e:
        print(f"Error querying table {table}: {e}")
        return [], []


def write_table_to_s3(s3_client, bucket_name, table, rows, columns, date_and_time):
    """Converts table data to JSON and uploads it to S3."""
    try:
        if not rows or not columns:
            print(f"Skipping {table}: No data to upload.")
            return None
        df = pd.DataFrame(data=rows, columns=columns)
        json_data = df.to_json(orient="records", lines=False, date_format="iso")
        key = f"data/{date_and_time}/{table}.json"
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json_data)
        return key
    except (ClientError, NoCredentialsError, ValueError, Exception) as e:
        print(f"Error writing {table} to S3: {e}")
        return None


def log_file(s3_client, bucket_name, keys):
    """Logs file upload details and writes to S3."""
    try:
        if not keys:
            print("No files were uploaded to log.")
            return None
        log_contents = []
        for key in keys:
            log_contents.append(f"Uploaded: {key} at {datetime.now()}")
        formatted_log = "\n".join(log_contents)
        bytes_log = str.encode(formatted_log)
        s3_client.put_object(
            Body=bytes_log,
            Bucket=bucket_name,
            Key=f"logs/{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.log",
        )
        return {"message": "Files Processed: Batch Lambda Transform complete"}
    except (ClientError, NoCredentialsError, Exception) as e:
        print(f"Error logging files to S3: {e}")
        return None


def json_to_pg8000_output(filepath, include_cols_in_output=True):
    """
    Reads the json and returns is as a nested list (the output format of p8000)
    Cols are also outputed as standard, as they will be needed as well

    """

    # Opening JSON file
    f = open(
        filepath,
    )
    simulated_pg8000_output = []
    simulated_pg8000_output_cols = []
    # returns JSON object as
    # a dictionary
    data = json.load(f)

    for i in data:
        simulated_pg8000_output += [list(i.values())]

    for i in data[0].keys():
        simulated_pg8000_output_cols += [i]

    # Closing file
    f.close()

    return simulated_pg8000_output, simulated_pg8000_output_cols


def return_week(date_isoformat_str):
    date_object = date.fromisoformat(date_isoformat_str)
    weekday_num = date_object.isoweekday()
    weekday_name_dict = {
        1: "monday",
        2: "tuesday",
        3: "wednesday",
        4: "thursday",
        5: "friday",
        6: "saturday",
        7: "sunday",
    }
    weekday_name = weekday_name_dict[weekday_num]
    return weekday_num, weekday_name


def return_s3_key(table_name, datetime_string, extension=".json"):
    return f"data/{datetime_string}/{table_name}{extension}"


def return_datetime_string():
    timestamp = datetime.now()
    year, month, day, hour, minute = (
        timestamp.year,
        timestamp.month,
        timestamp.day,
        timestamp.hour,
        timestamp.minute,
    )
    return f"{year}-{month}-{day}_{hour}-{minute}"


def simple_read_parquet_file_into_dataframe(bucket_name, key, s3_client):
    """source: https://stackoverflow.com/questions/51027645/how-to-read-a-single-parquet-file-in-s3-into-pandas-dataframe-using-boto3"""
    # Read the parquet file
    buffer = io.BytesIO()
    object = s3_client.Object(bucket_name, key)
    object.download_fileobj(buffer)
    df = pd.read_parquet(buffer)
    return df
