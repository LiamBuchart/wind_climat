"""

    Generate a climatology of wind direction data, not just the mean direction
    Save station interpolated (using KDTree) for every hour of each year

    output - two .csv files with stations as columns, times as rows
             first is stored wind direction
             second is stored wind speed

    Liam.Buchart@nrcan-rncan.gc.ca
    September 15, 2025

"""
#%%
import os
import xarray as xr
import requests
import shutil
import numpy as np
import pandas as pd
import json

from scipy.spatial import cKDTree
from concurrent.futures import ThreadPoolExecutor, as_completed

# set the month and year to take the average of
month = [1, 2]  # np.arange(1, 12+1)
year = 1990

data_dir = "./temp/"

# get the stinky variable names that casr uses - dont need to type them out here
with open("./utils/variables.json", 'r') as f:
    casr_vars = json.load(f)

# open the allstn2025 file to get stations names and locations
stations = pd.read_csv("./utils/allstn2025.csv")

# clean up the messy dataframe - note even necessary i just hated looking at it
# column dropped: [instr, tz_correct, h_bul, s_bul, hly, syn, tmm, ua, useindex]
stations = stations.drop(["instr", "tz_correct", "h_bul", "s_bul", "hly", "syn", "tmm", "ua", "useindex"], 
                         axis=1)

# now just the msc stations - these are most reliable and provides a solid list
# disregard the awful naming convention for the agency names
stn_df = stations[(stations['agency'] == 'MSC   ') | (stations['agency'] == 'ParksC')]
print(stn_df.head())

#%%
# now initialize two different empty dataarrays to store values in
# these will be large (~1000 columns for each station and then hourly output)
direction_df = pd.DataFrame(columns=list(stn_df['wmo'].values))
speed_df = pd.DataFrame(columns=list(stn_df['wmo'].values))

for ii in range(0, len(stn_df)):
    # add attributes containing station location and other info
    st = stn_df.iloc[ii]
    text = f"{st["name"]}_prov_{st["prov"]}_id_{st["id"]}_elev_{st["elev"]}".replace(" ", "")

    direction_df[st['wmo']].attrs['description'] = text
    direction_df[st['wmo']].attrs['lat'] = st['lat']
    direction_df[st['wmo']].attrs['lon'] = st['lon']

    speed_df[st['wmo']].attrs['description'] = text
    speed_df[st['wmo']].attrs['lat'] = st['lat']
    speed_df[st['wmo']].attrs['lon'] = st['lon']

#%%
print(speed_df.head())
#%%
def wind_dir_interp(da, var_name, point):
    # input:
    # wd_da ==> DataArray of wind directions
    # var_name ==> String for either variable (read from vraibles.json in utils)
    # point ==> Tuple of the point to interpolate to

    # get the data tha we need
    times = list(da['time'].values)

    # now have to interpolate at each time in the dataarray
    array = wd_da[var_name].values.flatten()

    lats = da.coords["lat"].values.flatten()
    lons = da.coords["lon"].values.flatten()

    coords = np.column_stack( (lats, lons) )

    # KDTree interpolation
    tree = cKDTree(coords)
    dist, idx = tree.query( (point[0], point[1]) )

    return array[idx]


#%%
# loop through each month in a year to collect all wind speeds and directions at msc stations   
for mm in month:
    file_name = f"{year}_{mm:02d}_station_wind_direction.nc"

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
            print("Current time: ", time)

            ds = xr.open_dataset(f"{data_dir}{mf}", engine="netcdf4")

            # wind speed
            ws_da = ds[casr_vars["CaSR_Variables"]["wind_speed"]] 
            ws_da_km = ws_da * 1.852  # convert to km/h from kts

            # wind direction
            wd_da = ds[casr_vars["CaSR_Variables"]["wind_direction"]]

            # interpolate both DataArrays - parallize due to the number of stations
            # parallization cause this thang is slooow
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(wind_dir_interp, wd_da, f"{date}12.nc") for date in dates]
                for future in as_completed(futures):
                    result = future.result()  # handle result or exceptions if needed  
    
