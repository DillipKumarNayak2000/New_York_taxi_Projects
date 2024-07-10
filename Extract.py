import os
import requests
from retrying import retry
import pandas as pd
import pyarrow.parquet as pq

# Base URL where PARQUET files are located
base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

# Directory to save downloaded files
save_dir = 'converted_csv/'  

# Function to download file with retry mechanism
@retry(wait_fixed=2000, stop_max_attempt_number=3)
def download_file(url, save_path):
    response = requests.get(url)
    response.raise_for_status()
    with open(save_path, 'wb') as f:
        f.write(response.content)

# Function to convert PARQUET file to CSV
def convert_parquet_to_csv(parquet_file, csv_file):
    try:
        # Read PARQUET file into a Pandas DataFrame
        df = pq.read_table(parquet_file).to_pandas()

        # Save as CSV file
        df.to_csv(csv_file, index=False)
        print(f"Converted {parquet_file} to {csv_file}")
    except Exception as e:
        print(f"Error converting {parquet_file} to CSV: {e}")

# Ensure save directory exists
os.makedirs(save_dir, exist_ok=True)

# Function to download PARQUET files for a given month
def download_and_convert_parquet_files(year, month):
    # List of types of data to download
    datasets = ['yellow_tripdata', 'green_tripdata', 'fhv_tripdata']

    for dataset in datasets:
        filename = f'{dataset}_{year}-{month:02d}.parquet'
        file_url = os.path.join(base_url, filename)
        parquet_save_path = os.path.join(save_dir, filename)
        csv_save_path = os.path.join(save_dir, f'{dataset}_{year}-{month:02d}.csv')

        try:
            print(f"Downloading {filename}...")
            download_file(file_url, parquet_save_path)
            print(f"Downloaded {filename} to {parquet_save_path}")

            # Convert PARQUET to CSV
            convert_parquet_to_csv(parquet_save_path, csv_save_path)

        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Iterate over months in year 2019 (assuming January to December)
for month in range(1, 13):
    download_and_convert_parquet_files(2019, month)
