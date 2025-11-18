"""

    Download 30 years of CaSR data from one day

    Liam.Buchart@nrcan-rncan.gc.ca
    September 17, 2025
"""
#%%
import requests
import os
import shutil

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from utils import get_date_from_years

# %%
def download_data(link, file_name):
    # check if it exists
    full_file_path = os.path.join("./temp", file_name)
    if os.path.exists(full_file_path):
        print(f"{full_file_path} already exists. Skipping...")
    else:
        try:
            # Send a GET request to download the file
            print(f"Starting Request for {file_name}: ")
            response = requests.get(f"{link}{file_name}", stream=True)
            response.raise_for_status()  # Check for HTTP request errors

            # Write the content to a local file
            with open(file_name, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            print(f"File downloaded successfully: {file_name}, moving to ./temp/ ...")    

            dst_dir = "./temp"

            # move the file to the temp directory 
            shutil.move(file_name, os.path.join(dst_dir, file_name))

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}") 

#%%
# parallization cause this thang is slooow
def run_parallel(dates, link):
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(download_data, link, f"{date}12.nc") for date in dates]
        for future in as_completed(futures):
            result = future.result()  # handle result or exceptions if needed


