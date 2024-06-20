import os
import boto3
from datetime import datetime

def download_missing_files_from_s3(local_directory, bucket_name, s3_prefix, aws_profile):
    """
    Download missing files from an AWS S3 bucket to a local directory.

    Args:
    local_directory (str): The local directory to check and save downloaded files.
    bucket_name (str): The name of the S3 bucket.
    s3_prefix (str): The prefix to list objects in S3 (e.g., 'us_stocks_sip/trades_v1/2024/02/').
    aws_profile (str): The AWS CLI profile name to use.
    """
    session = boto3.Session(profile_name=aws_profile)
    s3 = session.client('s3', endpoint_url='https://files.polygon.io')
    
    # List all files in the local directory
    local_files = set(os.listdir(local_directory))
    print(f"Local files: {local_files} \n")
    
    # List all files in the S3 bucket for the given prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    s3_files = sorted(set(item['Key'] for item in response.get('Contents', [])))
    print(f"S3 Files: {s3_files} \n")
    # Extract just the filenames from the S3 keys
    s3_filenames = set(os.path.basename(key) for key in s3_files)
    
    # Find which S3 files are missing locally
    missing_files = sorted(s3_filenames - local_files)
    print((f"Files to download: {missing_files} \n"))
    
    # Download the missing files
    for file_key in sorted(s3_files):
        filename = os.path.basename(file_key)
        if filename in missing_files:
            local_filepath = os.path.join(local_directory, filename)
            print(f"Downloading {filename} to {local_filepath} at {datetime.now()}...")
            s3.download_file(bucket_name, file_key, local_filepath)
            print(f"Download complete at {datetime.now()}")

# # Example usage
# local_directory = '/Volumes/WD18TB/us_stocks/trades/2024/03'
# bucket_name = 'flatfiles'
# s3_prefix = 'us_stocks_sip/trades_v1/2024/03/'
# aws_profile = 'polygon'

# download_missing_files_from_s3(local_directory, bucket_name, s3_prefix, aws_profile)

# Example usage that loops through years and months
base_local_directory = '/Users/ec2-user/stonkbot_data/us_stocks/trades'
base_s3_prefix = 'us_stocks_sip/trades_v1'
base_local_directory = '/Volumes/WD18TB/us_options/minute_aggs'
base_s3_prefix = 'us_options_opra/minute_aggs_v1'
base_local_directory = "/Users/brandon/Documents/polygon_data/minute_aggs/"
base_s3_prefix = 'us_stocks_sip/minute_aggs_v1'

bucket_name = 'flatfiles'
aws_profile = 'polygon'

# Loop through the years
start_date = datetime(2024, 4, 1)  # Start from March 2022

for year in range(start_date.year, 2025):
    # Loop through all months
    start_month = start_date.month if year == start_date.year else 1  # Start from March for the first year
    for month in range(start_month, 13):
        # Format the month to ensure it's two digits
        month_str = f'{month:02d}'
        
        # Construct the local directory path for the current year and month
        local_directory = os.path.join(base_local_directory, str(year), month_str)
        
        # Make sure the local directory exists, create if it does not
        os.makedirs(local_directory, exist_ok=True)
        
        # Construct the S3 prefix for the current year and month
        s3_prefix = f'{base_s3_prefix}/{year}/{month_str}/'
        
        # Call the download function for the current year and month
        print(f"Starting download for {year}-{month_str}")
        download_missing_files_from_s3(local_directory, bucket_name, s3_prefix, aws_profile)
        print(f"Completed download for {year}-{month_str}")

