# import libraries and include in requirements.txt
import os
from dotenv import load_dotenv
import pandas as pd
import boto3
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
import json

# load environment variables from .env file
load_dotenv()

# Mysql Database Connection Settings
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PORT = int(os.getenv('MYSQL_PORT'))
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_TABLE = os.getenv('MYSQL_TABLE')
CREATED_AT_DATE = os.getenv('CREATED_AT_DATE')

#Export Settings
EXPORT_FORMAT = os.getenv('EXPORT_FORMAT')
MIN_AGE_HOURS = int(os.getenv('MIN_AGE_HOURS'))

# Build SQLAlchemy connection string
conn = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

# AWS S3 Connection Settings
# eg. AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Validate the required environment variables
# - Ensure that all necessary environment variables are set before proceeding
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

# define the export function
# - Export Mysql table data to specified format (Parquet or JSON) files grouped by CREATED_AT_DATE date,
#   and upload them to AWS S3 bucket using a structured path.
def export_data():
    query = f"SELECT * FROM {MYSQL_TABLE}"
    df = pd.read_sql(query, conn)

    # Convert to datetime and ensure UTC
    df[CREATED_AT_DATE] = pd.to_datetime(df[CREATED_AT_DATE], utc=True)

    if 'context' in df.columns:
        df['context'] = df['context'].apply(double_json_load)

    # Filter rows older than MIN_AGE_HOURS
    current_time = datetime.now(timezone.utc)
    cutoff_time = current_time - timedelta(hours=MIN_AGE_HOURS)
    df = df[df[CREATED_AT_DATE] <= cutoff_time]

    if df.empty:
        print(f"No data older than {MIN_AGE_HOURS} hours to export.")
        return

    # Group by just the date part
    df['created_date'] = df[CREATED_AT_DATE].dt.date

    for date, group in df.groupby('created_date'):
        date_str = date.strftime('%Y-%m-%d')
        timestamp_str = current_time.strftime('%H-%M-%S')
        filename = f"{MYSQL_TABLE}_{date_str}_{timestamp_str}"

        if EXPORT_FORMAT == "json":
            file_path = export_as_json(group, filename)
            s3_key = f"{MYSQL_TABLE}/json/{date_str}/{filename}.json"
        elif EXPORT_FORMAT == "parquet":
            file_path = export_as_parquet(group, filename)
            s3_key = f"{MYSQL_TABLE}/parquet/{date_str}/{filename}.parquet"
        else:
            raise ValueError("Unsupported format. Use 'json' or 'parquet'.")

        success = upload_to_s3(file_path, s3_key)

        if success:
            delete_uploaded_rows(group)

def clean_nested_json_fields(df, column_names):
    for col in column_names:
        df[col] = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) and x.strip().startswith('{') else x)
    return df

def double_json_load(value):
    """Safely parse a double-encoded JSON string into a Python object."""
    try:
        if isinstance(value, str):
            first = json.loads(value)
            if isinstance(first, str):
                return json.loads(first)
            return first
    except Exception:
        pass
    return value

# define the export_as_json function
# - Export data as JSON file
def export_as_json(df, filename):
    """
    Export the given DataFrame as a JSON file.
    Each record is one line (newline-delimited JSON).
    """
    file_path = f"{filename}.json"
    df = clean_nested_json_fields(df, ['context'])
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
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3.upload_file(file_path, S3_BUCKET_NAME, s3_key)
        print(f"Uploaded {file_path} to s3://{S3_BUCKET_NAME}/{s3_key}")
        return True
    except Exception as e:
        print(f"Failed to upload {file_path} to S3: {e}")
        return False


def delete_uploaded_rows(df_group):
    """
    Deletes rows from MySQL that were already exported.
    Assumes 'id' is the primary key of the table.
    """
    if 'id' not in df_group.columns:
        print("Cannot delete rows: 'id' column not found.")
        return

    ids = df_group['id'].tolist()
    id_placeholders = ','.join([':id' + str(i) for i in range(len(ids))])
    delete_query = f"DELETE FROM {MYSQL_TABLE} WHERE id IN ({id_placeholders})"

    params = {f'id{i}': id_val for i, id_val in enumerate(ids)}
    with conn.begin() as connection:
        connection.execute(text(delete_query), params)

    print(f"Deleted {len(ids)} rows from MySQL.")

if __name__ == "__main__":
    export_data()