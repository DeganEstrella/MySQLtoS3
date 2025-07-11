# import libraries and include in requirements.txt

import os
from dotenv import load_dotenv
import mysql.connector
import pandas as pd
import boto3
from datetime import datetime, timedelta, timezone

# load environment variables from .env file

load_dotenv()

# Mysql Database Connection Settings
# eg. MYSQL_HOST = os.getenv('MYSQL_HOST')

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PORT = int(os.getenv('MYSQL_PORT'))
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_TABLE = os.getenv('MYSQL_TABLE')

conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    port=MYSQL_PORT,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)


# AWS S3 Connection Settings
# eg. AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')


AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# BONUS: Validate the required environment variables
# - we want to ensure that all necessary environment variables are set before proceeding


def validate_env_vars():
    required_vars = [
        'MYSQL_HOST',
        'MYSQL_USER',
        'MYSQL_TABLE',
        'MYSQL_PORT',
        'MYSQL_PASSWORD',
        'MYSQL_DATABASE',
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_REGION',
        'S3_BUCKET_NAME'
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

validate_env_vars()

# define the export function - eg. def export_data():
# - Export Mysql table data to specified format (Parquet or JSON) files grouped by CREATED_AT date,
#   and upload them to AWS S3 bucket using a structured path.

def export_data(export_format, min_age_hours):
    query = f"SELECT * FROM {MYSQL_TABLE}"
    df = pd.read_sql(query, conn)

    # Convert to datetime and ensure UTC
    df['CREATED_AT'] = pd.to_datetime(df['CREATED_AT'], utc=True)

    # Filter rows older than min_age_hours
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=min_age_hours)
    df = df[df['CREATED_AT'] <= cutoff_time]

    if df.empty:
        print(f"ℹ️ No data older than {min_age_hours} hours to export.")
        return

    # Group by just the date part
    df['created_date'] = df['CREATED_AT'].dt.date

    for date, group in df.groupby('created_date'):
        date_str = date.strftime('%Y-%m-%d')
        filename = f"{MYSQL_TABLE}_{date_str}"

        if export_format == "json":
            file_path = export_as_json(group, filename)
            s3_key = f"{MYSQL_TABLE}/json/{date_str}/{filename}.json"
        elif export_format == "parquet":
            file_path = export_as_parquet(group, filename)
            s3_key = f"{MYSQL_TABLE}/parquet/{date_str}/{filename}.parquet"
        else:
            raise ValueError("Unsupported format. Use 'json' or 'parquet'.")

        upload_to_s3(file_path, s3_key)


# define the export_as_json function
# - Export data as JSON file

def export_as_json(df, filename):
    """
    Export the given DataFrame as a JSON file.
    Each record is one line (newline-delimited JSON).
    """
    file_path = f"{filename}.json"
    df.to_json(file_path, orient='records', lines=True)
    return file_path

# define the export_as_parquet function
# - Export data as Parquet file

def export_as_parquet(df, filename):
    """
    Export the given DataFrame as a Parquet file.
    """
    file_path = f"{filename}.parquet"
    df.to_parquet(file_path, engine='pyarrow', index=False)
    return file_path

# define upload_to_s3 function
# - Uploads a local file to AWS S3 at the given path

def upload_to_s3(file_path, s3_key):
    """
    Upload a local file to AWS S3.

    Parameters:
    - file_path: str, path to the local file to upload
    - s3_key: str, the destination path/key in the S3 bucket
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    try:
        s3.upload_file(file_path, S3_BUCKET_NAME, s3_key)
        print(f"✅ Uploaded {file_path} to s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload {file_path} to S3: {e}")


if __name__ == "__main__":
    #format and min_age_hours should be changed here.
    export_format = "json"
    min_age_hours = 24
    export_data(export_format, min_age_hours)

# Entry point of the application
