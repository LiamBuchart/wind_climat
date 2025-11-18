#%%
import os
import json

import xarray as xr

data_dir = "./climatology/old_daily/"

months = [1, 2]
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
        print(ws_list)

    if ws_list:
        ws_all = xr.concat(ws_list, dim="time")
        print(f"Calculating stats for month: {mm:02d} ...")
        ws_stats = all_stats(ws_all)
        # Save to file
        print("SAving...")
        print(ws_stats.data_vars)
        print("Checking...")
        print(ws_stats.dims)
        print("Checking again...")
        # Remove all non-dimension coordinates
        data_cleaned = ws_stats.drop_vars([var for var in ws_stats.coords if var not in ws_stats.dims])
        print(ws_stats.coords)

        encoding = {var: {"zlib": True, "complevel": 1, "chunksizes": (100, 100)} for var in ws_stats.data_vars}
        ws_stats.to_netcdf(f"{save_dir}/1990-2020_monthly_windspeed_m{mm:02d}.nc", 
                           engine="h5netcdf", encoding=encoding)
        
        print(f"Finished with m{mm:02d}...")
    else:
        print(f"No windspeed files found for month: {mm:02d}")

print("Complete")

# %%
import xarray as xr
import time
import matplotlib.pyplot as plt
all_files = os.listdir(data_dir)
month_files = [file for file in all_files if f"m01" in file]
speed_files = [file for file in month_files if "windspeed" in file]

ws_list = []
for file in speed_files:
    ds = xr.open_dataset(f"{data_dir}{file}", engine="netcdf4", chunks={})
    ws = ds[casr_vars["CaSR_Variables"]["wind_speed"]] # variable name from the json
    ws_list.append(ws)

ws_all = xr.concat(ws_list, dim="time")
# Optionally: ws_all = ws_all.persist()

print(ws_all)
#fig, axs = plt.subplots()
#ws_all.quantile(0.1, dim="time").plot()
#fig.show()

ws_mn = ws_all.mean(dim="time")
ws_std = ws_all.std(dim="time")
ws_10 = ws_all.quantile(0.1, dim="time") 
print(ws_10)

stats = xr.Dataset({
    "mean": ws_all.mean(dim="time"),
    "std": ws_all.std(dim="time"),
    #"p10": ws_all.quantile(0.1, dim="time"),
    # ...other stats
})

print(time.localtime())
#ws_comb = xr.merge([ws_mn, ws_std, ws_10])

#print(ws_comb)

encoding = {var: {'dtype': 'int16', 'scale_factor': 0.1, '_FillValue': -9999} for var in stats.ws_10}
#print(time.localtime())
ws_10.to_netcdf(f"{save_dir}output_p10.nc"), 
#                          encoding=encoding)
print(time.localtime())
# %%
