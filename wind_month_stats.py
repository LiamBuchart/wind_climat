#%%
import os
import json
import xarray as xr
import time
import numpy as np


# Set the month to process (e.g., February)
month = 4
month_str = f"m{month:02d}"
wind_var = "wind_speed"  # ["wind_speed", "wind_direction"]

data_dir = "climatology/daily/"
output_dir = "climatology/monthly/"

all_files = os.listdir(data_dir)

if wind_var == "wind_speed":
    output_file = f"{output_dir}/1990-2020_monthly_windspeed_stats_m{month:02d}.nc"
    wind_files = [file for file in all_files if month_str in file and "windspeed" in file]
elif wind_var == "wind_direction":
    output_file = f"{output_dir}/1990-2020_monthly_winddirection_stats_m{month:02d}.nc"
    wind_files = [file for file in all_files if month_str in file and "winddir" in file]
else: 
    print("Error: Not a correct variable selection.")

# get the variable names
with open("./utils/variables.json", 'r') as f:
    casr_vars = json.load(f)

# Find all windspeed files for the month
def squeeze_quantile(da):
    if "quantile" in da.dims and da.sizes["quantile"] == 1:
        da = da.squeeze("quantile")

    # remove the "quantile" coordinate which comes with the calculations
    # causes errors when combining datasets
    if "quantile" in da.coords:
        da = da.reset_coords("quantile", drop=True)
    return da

#%%
ws_list = []
start_time = time.time()
for file in wind_files:  
    ds = xr.open_dataset(os.path.join(data_dir, file), chunks={})
    #ds[casr_vars["CaSR_Variables"]["wind_speed"]].plot()
    ws = ds[casr_vars["CaSR_Variables"][wind_var]].compute()

    print(f"Trimming Dataset {file}...")
    # trim all lons >309 (-51W) to shrink file enough for compression eastern in canada
    trim_lon = 309
    ws = ws.where(ws['lon'] <= trim_lon, drop=True)
    trim_lon = 216  # (-144W) western in Canada
    ws = ws.where(ws['lon'] >= trim_lon, drop=True)

    ws_list.append(ws)

if ws_list:
    ws_all = xr.concat(ws_list, dim="time")

    mean = ws_all.mean(dim="time")
    median = ws_all.median(dim="time")
    std = ws_all.std(dim="time")
    max = ws_all.max(dim="time")

    p10 = squeeze_quantile(ws_all.quantile(0.1, dim="time"))
    p25 = squeeze_quantile(ws_all.quantile(0.25, dim="time"))
    p75 = squeeze_quantile(ws_all.quantile(0.75, dim="time"))
    p90 = squeeze_quantile(ws_all.quantile(0.9, dim="time"))
    p95 = squeeze_quantile(ws_all.quantile(0.95, dim="time"))

    stats_ds = xr.Dataset({
        "mean": mean,
        "median": median,
        "std": std,
        "p10": p10,
        "p25": p25,
        "p75": p75,
        "p90": p90,
        "p95": p95,
        "max": max
    })

    print("Starting compression...")
    # Save to NetCDF (fast and widely supported)
    stats_ds.to_netcdf(output_file, engine="netcdf4")
    print(f"Saved stats to {output_file}")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Time to save: {int(elapsed_time)/60} minutes")
else:
    print("No windspeed files found for this month.")

stats_ds.close()
# %%
