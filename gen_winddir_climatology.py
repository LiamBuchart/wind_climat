"""

    Generate 30 year mean, median, and std, of wind direction, 
    for each month and meteorological season (DJF, MAM, JJA, SON).

    Input: nil
    Ouput: 1990-2020 monthly and seasonal mean files (in .h5 files)
           of wind direction.

    Liam.Buchart@nrcan-rncan.gc.ca
    September 12, 2025

"""
#%%
import os
import json

import xarray as xr

data_dir = "./climatology/daily/"

months = range(1, 12+1)
seasons = {
    "DJF": [12, 1, 2],
    "MAM": [3, 4, 5],
    "JJA": [6, 7, 8],
    "SON": [9, 10, 11]
}

# do the monthly means and stats first
save_dir = "./climatology/monthly/"

# get the variable names
with open("./utils/variables.json", 'r') as f:
    casr_vars = json.load(f)

# combine with time dim
def combine_da(count, da1):
    # combine the DataArrays
    if count == 0:
        da_all = da1
    else:
        da_all = xr.concat([da_all, da1], dim="time")
    return da_all

def all_stats(da):
    # get all the stats for a DataArray
    da_mean = da.mean(dim="time")
    da_median = da.median(dim="time")
    da_mode = da.mode(dim="time")
    da_std = da.std(dim="time")

    # combine into a dataset
    ds = xr.Dataset({
        "mean": da_mean,
        "median": da_median,
        "mode": da_mode,
        "std": da_std,
    })

    return ds

for mm in months:
    # get all the files for the month in the hrly directory
    all_files = os.listdir(data_dir)
    month_files = [file for file in all_files if f"m{mm:02d}" in file]

    count = 0
    for file in month_files:
        # concatenate all the files for the month
        print(f"Processing file: {file}")
        ds = xr.open_dataset(f"{data_dir}{file}", engine="h5netcdf")
        wd = ds[casr_vars["CaSR_Variables"]["wind_direction"]]

        wd_all = combine_da(count, wd)

        count += 1

    # now get the stats
    print(f"Calculating stats for month: {mm:02d} ...")
    wd_stats = all_stats(wd_all)

    # save the files
    wd_stats.to_netcdf(f"{save_dir}/1990-2020_monthly_winddir_m{mm:02d}.h5")

# now do the seasonal means and stats
save_dir = "./climatology/seasonal/"

for season, months in seasons.items():
    # get all the files for the season in the hrly directory
    all_files = os.listdir(data_dir)
    season_files = []
    for mm in months:
        season_files.extend([file for file in all_files if f"m{mm:02d}" in file])

    count = 0
    for file in season_files:
        # concatenate all the files for the month
        print(f"Processing file: {file}")
        ds = xr.open_dataset(f"{data_dir}{file}", engine="h5netcdf")
        wd = ds[casr_vars["CaSR_Variables"]["wind_direction"]]

        wd_all = combine_da(count, wd)

        count += 1

    # now get the stats
    print(f"Calculating stats for season: {season} ...")
    wd_stats = all_stats(wd_all)

    # save the files
    wd_stats.to_netcdf(f"{save_dir}/1990-2020_seasonal_winddir_{season}.h5")