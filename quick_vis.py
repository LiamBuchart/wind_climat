""""

    Just a sanity check on variables, quick plots using xarray

"""
#%%
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

import json

##### USER INPUT #####
data_dir = "./climatology"
year = 1990
month = 2

variable = "wind_direction"
##### END USER INPUT #####

with open("variables.json", 'r') as f:
    casr_vars = json.load(f)
var = casr_vars["Climate_Variables"][variable]

file = f"{data_dir}/{variable}_monthly_{year}-{month:02d}.h5"

ds = xr.open_dataset(file, engine="netcdf4")

# %%
# make the map
def quick_da_map(ds, var_name, title):
    plt.figure(figsize=(10, 8))
    # data resolution
    resol = '50m'
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_extent([-145, -45, 43, 75], crs=ccrs.PlateCarree())
    # province boundaries
    provinc_bodr = cfeature.NaturalEarthFeature(category='cultural', 
                    name='admin_1_states_provinces_lines', scale=resol, facecolor='none', edgecolor='k')
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle='-')
    ax.add_feature(provinc_bodr, linestyle='--', linewidth=0.6, edgecolor="k", zorder=10)
    ax.set_title(title)
    da = ds[var_name]
    da.plot.pcolormesh(ax=ax, 
                       transform=ccrs.PlateCarree(), 
                       x="lon", y="lat",
                       cmap="turbo")

    plt.colorbar(label=variable)
    plt.show()

# %%
quick_da_map(ds, var, f"{var}_{year}_{month}_Quick_Vis")
# %%
