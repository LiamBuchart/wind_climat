"""

    Generate Monthly and seasonal (DJF, MAM, JJA, SON) means of 
    wind run, wind accumulation, and genral wind speed + direction.

    Liam.Buchart@nrcan-rncan.gc.ca
    September 10, 2025

"""
#%%
import xarray as xr
import pandas as pd
import numpy as np
import os
import json

from concurrent.futures import ThreadPoolExecutor, as_completed

# set the month and year to take the average of
month = np.arange(1, 12+1)
year = 1990

save_dir = "./climatology"
data_dir = "./temp/"

# get the stinky variable names that casr uses - dont need to type them out here
with open("./utils/variables.json", 'r') as f:
    casr_vars = json.load(f)

def month_mean(da1, da2):
    # use some nice array techniques to create a month mean
    return xr.concat([da1, da2], dim="time")

#%%
# loop through each month in a year to get a monthly mean (seasonal averages are created when the climatologies are)    
for mm in month:
    file_name = f"{year}_{mm:02d}_month_mean.nc"

    all_files = os.listdir(data_dir)

    # check if it exists
    full_file_path = os.path.join("./climatology", file_name)

    if os.path.exists(full_file_path):
        print(f"{full_file_path} already exists. Skipping...")
    else:
        # get just the daily files for our data and month
        print("Getting the month files together...")
        month_files = [file for file in all_files if f"{year}{mm:02d}" in file]

        day = 1  # dummy variable for the day
        # now loop through these files to create an array of wind speed, dir, and wind run
        for mf in month_files:
            time = f"{year}-{mm:02d}-{day:02d}"  

            # note that xarray requires a pandas timestamp
            timestamp = pd.Timestamp(time)
            #timestamp = xr.DataArray(pd.Timestamp(time), [( 'time', pd.Timestamp(time) )])
            print("Current time: ", time)

            ds = xr.open_dataset(f"{data_dir}{mf}", engine="netcdf4")

            # wind speed
            ws_da = ds[casr_vars["CaSR_Variables"]["wind_speed"]] 
            ws_da_km = ws_da * 1.852  # convert to km/h from kts

            # wind run
            wr_da = ws_da  # just copy for now

            # wind direction
            wd_da = ds[casr_vars["CaSR_Variables"]["wind_direction"]]

            if day == 1:
                print("First day of the month, nothing to average yet")
                ws_month = ws_da_km
                wr_month = wr_da
                wd_month = wd_da
            else:
                print("Concatenating along the time axis")
                ws_month = month_mean(da1=ws_month, da2=ws_da_km)
                wr_month = month_mean(da1=wr_month, da2=wr_da)
                wd_month = month_mean(da1=wd_month, da2=wd_da)

                print(ws_month.values.shape)

            # advance the day my good sir
            day += 1

        # mean over the time dimension
        mean_ws = ws_month.mean(dim="time")
        mean_wr = wr_month.sum(dim="time")  # can do this because of hourly data (km/h is just kms each hour, then sum)
        mean_wd = wd_month.mean(dim="time")

        print(mean_ws)

        # save each dataray to the save_dir folder
        mean_ws.to_netcdf(f"{save_dir}/wind_speed_monthly_{year}-{mm:02d}.h5")
        mean_wr.to_netcdf(f"{save_dir}/wind_run_monthly_{year}-{mm:02d}.h5")
        mean_wd.to_netcdf(f"{save_dir}/wind_direction_monthly_{year}-{mm:02d}.h5")
# %%
