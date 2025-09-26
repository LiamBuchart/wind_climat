"""

    Create 365(6) files of hourly wind data for the 30 year climatology
    Input: daily netcdf files from the CaSR dataset - only storing one year at a time
    Output: .h5 files of hourly wind speed (km/h), wind direction (degrees) for each day of the year

    Liam.Buchart@nrcan-rncan.gc.ca
    September 17, 2025

"""
#%%
import xarray as xr
import pandas as pd
import numpy as np
import os
import json

from concurrent.futures import ThreadPoolExecutor, as_completed

# set the date to process
month = 2
day = 18

save_dir = "./climatology/daily/"
data_dir = "./temp/"

# get the stinky variable names that casr uses - dont need to type them out here
with open("./utils/variables.json", 'r') as f:
    casr_vars = json.load(f)

all_files = os.listdir(data_dir)

# loop through each day in a year to get a daily file
day_files = [file for file in all_files if f"{month:02d}{day:02d}12.nc" in file]

#%%
count = 0
for file in day_files:
    # get the date info from the file name
    if int(month) < 12:
        sfilename = f"1990-2020_hrly_windspeed_m{month:02d}_d{day:02d}.h5"
        dfilename = f"1990-2020_hrly_winddir_m{month:02d}_d{day:02d}.h5"
    else: 
        # december is actualy an 1989-2019 climatology
        sfilename = f"1989-2019_hrly_windspeed_m{month:02d}_d{day:02d}.h5"
        dfilename = f"1989-2019_hrly_winddir_m{month:02d}_d{day:02d}.h5"

    if os.path.exists(os.path.join(save_dir, sfilename)) and os.path.exists(os.path.join(save_dir, dfilename)):
        print(f"{sfilename} and {dfilename} already exists. Skipping...")
    else:
        print(f"Generating the hrly wind files ...")
        print(f"Current file: {file}")

        ds = xr.open_dataset(f"{data_dir}{file}", engine="netcdf4")

        # wind speed
        ws_da = ds[casr_vars["CaSR_Variables"]["wind_speed"]] 
        ws_da = ws_da * 1.852  # convert to km/h from kts

        # wind direction
        wd_da = ds[casr_vars["CaSR_Variables"]["wind_direction"]]

        if count == 0:
            # first date
            ws_all = ws_da
            wd_all = wd_da
        else:
            # concatenate the data arrays
            ws_all = xr.concat([ws_all, ws_da], dim="time")
            wd_all = xr.concat([wd_all, wd_da], dim="time")

            print(ws_all.values.shape)

    count += 1

# save the files
# encoding comes from recommended xarray compressions for netcdf files
if os.path.exists(os.path.join(save_dir, sfilename)) and os.path.exists(os.path.join(save_dir, dfilename)):
    print(f"Done here...")
else: 
    ws_all.to_netcdf(f"{save_dir}/{sfilename}", 
                 encoding={casr_vars["CaSR_Variables"]["wind_speed"]: {
                     'dtype': 'int16', 'scale_factor': 0.1, '_FillValue': -9999}})
    wd_all.to_netcdf(f"{save_dir}/{dfilename}", 
                 encoding={casr_vars["CaSR_Variables"]["wind_direction"]: {
                     'dtype': 'int16', 'scale_factor': 0.1, '_FillValue': -9999}})

print("Complete")
# %%
