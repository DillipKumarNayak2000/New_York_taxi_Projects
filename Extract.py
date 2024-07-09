import os
import requests
from retry import retry
from requests.exceptions import RequestException

# Function to download CSV file from URL with retries for network errors
@retry(RequestException, tries=3, delay=2, backoff=2)
def download_csv(url, output_folder):
    # Extract filename from URL
    filename = url.split('/')[-1]
    # Construct output file path
    output_path = os.path.join(output_folder, filename)

    try:
        # Download CSV file
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, stream=True, headers=headers)
        response.raise_for_status()  # Raise error for bad response status

        # Save CSV file
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Downloaded {filename}")

    except RequestException as e:
        print(f"Error downloading {url}: {e}")
        raise  # Re-raise the exception for retry decorator
    except Exception as e:
        print(f"Unexpected error processing {url}: {e}")

# Base URL for CSV files
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Year and months for 2019
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

# Output folder for CSV files
output_folder = 'yellow_tripdata_2019'

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

# Iterate through each month in 2019
for month in months:
    csv_url = f"{base_url}yellow_tripdata_2019-{month}.csv"

    try:
        download_csv(csv_url, output_folder)
    except RequestException as e:
        print(f"Error downloading {csv_url}: {e}")
    except Exception as e:
        print(f"Unexpected error processing {csv_url}: {e}")
