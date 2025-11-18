"""

    Similar to gen_climatology.py but getting windrun which requires so 
    additional processing since stats come from month total rather than hourly values.
    Calculate mean, mdian, std, 10th, 25th, 75th, 90th, 95th percentiles
    for each month and meteorological season (DJF, MAM, JJA, SON).

    Liam.Buchart@nrcan-rncan.gc.ca
    September 19, 2025

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
years = range(1990, 2020+1)
dec_years = range(1989, 2019+1)

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
    da_std = da.std(dim="time")
    da_p10 = da.quantile(0.1, dim="time")
    da_p25 = da.quantile(0.25, dim="time")
    da_p75 = da.quantile(0.75, dim="time")
    da_p90 = da.quantile(0.9, dim="time")
    da_p95 = da.quantile(0.95, dim="time")

    # combine into a dataset
    ds = xr.Dataset({
        "mean": da_mean,
        "median": da_median,
        "std": da_std,
        "p10": da_p10,
        "p25": da_p25,
        "p75": da_p75,
        "p90": da_p90,
        "p95": da_p95
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
        ws = ds[casr_vars["CaSR_Variables"]["wind_speed"]]

        # for year in years??

        count += 1

    # now get the stats
    print(f"Calculating stats for month: {mm:02d} ...")
    ws_stats = all_stats(ws_all)

    # save the files
    ws_stats.to_netcdf(f"{save_dir}/1990-2020_monthly_windspeed_m{mm:02d}.h5")

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
        ws = ds[casr_vars["CaSR_Variables"]["wind_speed"]]

        ws_all = combine_da(count, ws)

        count += 1

    # now get the stats
    print(f"Calculating stats for season: {season} ...")
    ws_stats = all_stats(ws_all)


    # save the files
    ws_stats.to_netcdf(f"{save_dir}/1990-2020_seasonal_windspeed_{season}.h5")
