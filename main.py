# import libraries and include in requirements.txt



# load environment variables from .env file



# Mysql Database Connection Settings
# eg. MYSQL_HOST = os.getenv('MYSQL_HOST')



# AWS S3 Connection Settings
# eg. AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')



# BONUS: Validate the required environment variables
# - we want to ensure that all necessary environment variables are set before proceeding



# define the export function - eg. def export_data():
# - Export Mysql table data to specified format (Parquet or JSON) files grouped by CREATED_AT date,
#   and upload them to AWS S3 bucket using a structured path.



# define the export_as_json function
# - Export data as JSON file



# define the export_as_parquet function
# - Export data as Parquet file



# define upload_to_s3 function
# - Uploads a local file to AWS S3 at the given path



# Entry point of the application
