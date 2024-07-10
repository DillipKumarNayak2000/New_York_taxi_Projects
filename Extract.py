import os
import requests
from retrying import retry
import pandas as pd


# Base URL where PARQUET files are located
base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

# Directory to save downloaded files
save_dir = 'converted_csv/'  # Change this to your desired directory path

# Function to download file with retry mechanism
@retry(wait_fixed=2000, stop_max_attempt_number=3)
def download_file(url, save_path):
    response = requests.get(url)
    response.raise_for_status()
    with open(save_path, 'wb') as f:
        f.write(response.content)

# Ensure save directory exists
os.makedirs(save_dir, exist_ok=True)

# Function to download PARQUET files for a given month
def download_parquet_files(year, month):
    # List of types of data to download
    datasets = ['yellow_tripdata', 'green_tripdata', 'fhv_tripdata']

    for dataset in datasets:
        filename = f'{dataset}_{year}-{month:02d}.parquet'
        file_url = os.path.join(base_url, filename)
        save_path = os.path.join(save_dir, filename)

        try:
            print(f"Downloading {filename}...")
            download_file(file_url, save_path)
            print(f"Downloaded {filename} to {save_path}")
        except Exception as e:
            print(f"Error downloading {filename}: {e}")

# Iterate over months in year 2019 (assuming January to December)
for month in range(1, 13):
    download_parquet_files(2019, month)
