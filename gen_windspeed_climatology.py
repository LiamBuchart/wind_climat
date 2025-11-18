"""

    Generate 30 year mean, median, std, 25th, 75th, 
    10th, and 90th percentiles of wind speed 
    for each month and meteorological 
    season (DJF, MAM, JJA, SON).

    Input: nil
    Ouput: 1990-2020 monthly and seasonal mean files (in .nc files)
           of wind speed, direction, and run.

    Liam.Buchart@nrcan-rncan.gc.ca
    September 12, 2025

"""
#%%
import os
import json

import xarray as xr

data_dir = "./climatology/daily/"

months = [3, 4]  #range(1, 12+1)
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
def all_stats(da):
    # Empty dataset
    ds = xr.Dataset()

    ds["mean"] = da.mean(dim="time")
    ds["median"] = da.median(dim="time")
    ds["std"] = da.std(dim="time")

    print(ds["median"].shape, ds["std"].shape)

    # Quantiles
    for q, name in zip([0.1, 0.25, 0.75, 0.9, 0.95], ["p10", "p25", "p75", "p90", "p95"]):
        print(ds.keys(), q, name)
        quant = da.quantile(q, dim="time")
        ds[name] = quant

    return ds

#%%
for mm in months:
    # get all the files for the month in the hrly directory
    all_files = os.listdir(data_dir)
    month_files = [file for file in all_files if f"m{mm:02d}" in file]
    speed_files = [file for file in month_files if "windspeed" in file]

    # Use Dask to open datasets lazily and concatenate
    ws_list = []
    for file in speed_files:
        print(f"Processing file: {file}")
        ds = xr.open_dataset(f"{data_dir}{file}", engine="netcdf4", chunks={})
        ws = ds[casr_vars["CaSR_Variables"]["wind_speed"]]
        ws_list.append(ws)

    if ws_list:
        ws_all = xr.concat(ws_list, dim="time")
        print(f"Calculating stats for month: {mm:02d} ...")
        ws_stats = all_stats(ws_all)

        # Compute and save to file
        # Remove all non-dimension coordinates
        data_cleaned = ws_stats.drop_vars([var for var in ws_stats.coords if var not in ws_stats.dims])
        print(ws_stats.coords)

        encoding = {var: {"chunksizes": (100, 100), 'dtype': 'int16', 'scale_factor': 0.1, '_FillValue': -9999} 
                    for var in ws_stats.data_vars}  # "zlib": True, "complevel": 1, 
        ws_stats.to_netcdf(f"{save_dir}/1990-2020_monthly_windspeed_m{mm:02d}.nc", 
                           engine="h5netcdf", encoding=encoding)
        print(f"Finished with m{mm:02d}...")

    else:
        print(f"No windspeed files found for month: {mm:02d}")

# now do the seasonal means and stats
save_dir = "./climatology/seasonal/"

#%%
for season, months in seasons.items():
    # get all the files for the season in the hrly directory
    all_files = os.listdir(data_dir)
    season_files = []
    for mm in months:
        season_files.extend([file for file in all_files if f"m{mm:02d}" in file])

    # Use Dask to open datasets lazily and concatenate
    ws_list = []
    for file in season_files:
        print(f"Processing file: {file}")
        ds = xr.open_dataset(f"{data_dir}{file}", engine="h5netcdf", chunks={})
        ws = ds[casr_vars["CaSR_Variables"]["wind_speed"]]
        ws_list.append(ws)

    if ws_list:
        ws_all = xr.concat(ws_list, dim="time")
        print(f"Calculating stats for season: {season} ...")
        ws_stats = all_stats(ws_all)

        # Compute and save to file
        encoding = {var: {"zlib": True, "complevel": 1, "chunksizes": (100, 100)} for var in ws_stats.data_vars}
        ws_stats.to_netcdf(f"{save_dir}/1990-2020_seasonal_windspeed_m{season:02d}.nc", 
                           engine="h5netcdf", encoding=encoding)
        print(f"Finished with {season:02d}...")
    else:
        print(f"No windspeed files found for season: {season}")
# %%
