"""

    Two different functions:
    get_station_data(name) - get station data from the cwfis database
    get_casr_data(lat, lon) - get the casr data from the netcdf files based on lat/lon

    Liam.Buchart@nrcan-rncan.gc.ca
    September 24, 2025

"""
#%%
import pandas as pd
import numpy as np
import psycopg2
import paramiko
import json
import csv
import sshtunnel
import calendar
import os

from datetime import datetime, timedelta

def last_day_of_month(year, month):
    return calendar.monthrange(year, month)[1]


def set_query(start, end, stationid):
    """
    start + end - strings YYYY-MM-DD
    station_name - string
    output: SQL query string
    """
    # query from can_hly2020s
    QUERY = f"SELECT rep_date, ws, wg, wdir FROM can_hly2020s WHERE wmo = '{stationid}' AND rep_date BETWEEN '{start} 00:00:00' AND '{end} 23:00:00' ORDER BY rep_date;" 

    return QUERY

def set_areal_query(mstart, mend, year, bbox):
    """
    mstart + mend - strings digit of the month MM or M
    year - string YYYY
    bbox - dictionary of bounding box from which to grab stations
    output: SQL query string
    """

    min_lon = bbox["west"]
    max_lon = bbox["east"]

    min_lat = bbox["south"]
    max_lat = bbox["north"]

    dend = last_day_of_month(year, mend)

    # break query down into smaller bits
    q1 = f"SELECT cwfis_allstn{year}.aes, lat, lon, elev, ws, wg, wdir, from cwfis_allstn{year}, can_hly2020s where "
    q2 = f"cwfis_allstn{year}.aes in (SELECT aes from cwfis_allstn{year} where "
    q3 = f"lon between {min_lon} and {max_lon} and lat between {min_lat} and {max_lat}) "
    q4 = f"and rep_date between '{year}-{mstart:02d}-01 00:00:00' and {year}-{mend:02d}-{dend} 23:00:00' and "
    q5 = f"cwfis_allstn{year}.aes = can_hly2020s order by rep_date, cwfis_allstn{year}.aes;"

    # query from can_hly2020s
    QUERY = q1 + q2 + q3 + q4 + q5

    return QUERY


#%%
def db_query(query, csv_output='query_output.csv'):
    """
    Call the database to get wind data
    Inut: cursor object (defined below)
          start and end [dates YYYY-MM-DD - string]
    Output: pandas dataframe
    """
    # open the .keys json file
    with open('../utils/.keys.json', 'r') as f:
        keys = json.load(f)

    # dagan info
    hostname = keys["dagan"]["full_name"]
    user = keys["dagan"]["user"]
    pw = keys["dagan"]["pw"]

    # database info
    d_hostname = keys["database"]["hostname"]
    d_username = keys["database"]["user"]
    db_name = keys["database"]["name"]
    d_pw = keys["database"]["pw"]

    portnum = 22  # just lookedup in my putty session

    # connect to remote database
    with sshtunnel.open_tunnel(
        (hostname, portnum),
        ssh_username=user,
        ssh_password=pw,
        remote_bind_address=(d_hostname, 5432)
    ) as tunnel:
        try:
            print("SSH tunnel established")
            print(f"{d_hostname, tunnel.local_bind_port}")
            print(f"Connecting to database {db_name} as user {d_username}")
            conn = psycopg2.connect(
                host=d_hostname,
                port=5432,
                database=db_name,
                user=d_username,
                password=d_pw
            )  

            cur = conn.cursor()
            # start by setting the search path
            cur.execute("set search_path to bt;")
            cur.execute(query)
            rows = cur.fetchall()  

            colnames = [desc[0] for desc in cur.description]
            with open(csv_output, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(colnames)
                writer.writerows(rows)
            cur.close()
            conn.close()

            print(f"Query results saved to {csv_output}")

        except Exception as e:
            print("Error:", e) 


#%%
def mm_dd_pairs(start_date, end_date, date_format='%Y-%m-%d', as_int=False):
    """
    Return a tuple of (mm, dd) pairs for every date from start_date to end_date inclusive.

    Parameters
    - start_date, end_date: str or datetime/date (if str, parsed with date_format)
    - date_format: format used to parse string dates (default '%Y-%m-%d')
    - as_int: if True return integers (month, day), else zero-padded strings ('MM','DD')

    Returns
    - tuple of (mm, dd) pairs in chronological order
    """
    # parse / normalize
    if isinstance(start_date, str):
        start = datetime.strptime(start_date, date_format).date()
    else:
        start = start_date if hasattr(start_date, 'date') else start_date

    if isinstance(end_date, str):
        end = datetime.strptime(end_date, date_format).date()
    else:
        end = end_date if hasattr(end_date, 'date') else end_date

    if end < start:
        raise ValueError("end_date must be >= start_date")

    result = []
    cur = start
    one_day = timedelta(days=1)
    while cur <= end:
        if as_int:
            pair = (cur.month, cur.day)
        else:
            pair = (f"{cur.month:02d}", f"{cur.day:02d}")
        result.append(pair)
        cur += one_day

    return tuple(result)  


# compute percentiles from a 1D array
def pct_from_vals(vals):
    vals = np.asarray(vals).ravel()
    vals = vals[~np.isnan(vals)]
    if vals.size == 0:
        return { 'p10': np.nan, 'p25': np.nan, 'p50': np.nan, 'p75': np.nan, 'p90': np.nan }
    qs = [10,25,50,75,90, 95]
    pv = np.nanpercentile(vals, qs)
    return {f'p{q}': float(v) for q,v in zip(qs, pv)}


def nearest_points(lat_coord, lon_coord, target_lat, target_lon, k=4):
    """
    Find k nearest grid points to (target_lat, target_lon).
    Returns list of dicts with lat/lon, flat_index, ij, distance_deg.
    """
    def to_numpy(a):
        try:
            vals = a.values
        except Exception:
            vals = np.asarray(a)
        if hasattr(vals, "compute"):
            vals = vals.compute()
        return np.asarray(vals)

    lat = to_numpy(lat_coord)
    lon = to_numpy(lon_coord)

    if lat.ndim == 1 and lon.ndim == 1:
        lon2d, lat2d = np.meshgrid(lon, lat)
    elif lat.ndim == 2 and lon.ndim == 2:
        lat2d = lat
        lon2d = lon-360  # sits in a 0-360 format want -180-180
    else:
        raise ValueError("lat/lon must be both 1D or both 2D arrays")

    points = np.column_stack((lat2d.ravel(), lon2d.ravel()))
    try:
        from scipy.spatial import cKDTree
        tree = cKDTree(points)
        dists, idxs = tree.query((target_lat, target_lon), k=k)
        if k == 1:
            dists = np.atleast_1d(dists); idxs = np.atleast_1d(idxs)
    except Exception:
        diffs = points - np.asarray([target_lat, target_lon])
        d2 = (diffs**2).sum(axis=1)
        idxs = np.argpartition(d2, range(k))[:k]
        idxs = idxs[np.argsort(d2[idxs])]
        dists = np.sqrt(d2[idxs])

    nlat, nlon = lat2d.shape
    out = []
    for idx, dist in zip(idxs, dists):
        i, j = divmod(int(idx), nlon)
        out.append({
            "lat": float(lat2d[i, j]),
            "lon": float(lon2d[i, j]),
            "flat_index": int(idx),
            "ij": (int(i), int(j)),
            "distance_deg": float(dist)
        })
    out = sorted(out, key=lambda x: x["distance_deg"])
    return out


# %%
from etl_station_data import mm_dd_pairs, pct_from_vals
import numpy as np
import pandas as _pd
from scipy.interpolate import griddata
from pathlib import Path
import xarray as xr

def get_casr_data(start, end, lat, lon):
    """
    Input: lat, lon - floats
           start, end - strings YYYY-MM-DD
    Output: list of neighbour dicts each containing timeseries ('times','values') across the date range.
    """
    
    dates = mm_dd_pairs(start, end, date_format='%Y-%m-%d', as_int=False)
    with open("../utils/variables.json", 'r') as f:
        casr_vars = json.load(f)
    data_dir = Path("../climatology/daily").resolve(strict=False)
    all_files = os.listdir(str(data_dir))
    speed_files = [file for file in all_files if "windspeed" in file]
    direc_files = [file for file in all_files if "winddir" in file]

    # create a dataframe to store this output in
    results = pd.DataFrame(columns=["idx", "lat", "lon", "ij", "distance", "t0", "dates", "speed", "direction"])

    # We'll accumulate time/value lists per neighbor using flat_index as key
    neighbor_series = {}
    neighbor_order = []  # to preserve ordering from first file
    for ii in range(len(dates)):
        mon, day = dates[ii]
        mon_str = f"_m{mon}"
        day_str = f"_d{day}"

        spd_file = [file for file in speed_files if mon_str in file and day_str in file]
        dir_file = [file for file in direc_files if mon_str in file and day_str in file]

        if not spd_file or not dir_file:
            print(f"Missing files for {mon_str}{day_str}")
            continue

        spd_path = os.path.join(str(data_dir), spd_file[0])
        dir_path = os.path.join(str(data_dir), dir_file[0])
        print("Files to open: ", spd_path, dir_path)

        dir_ds = xr.open_dataset(dir_path, chunks={})
        dir_var = casr_vars["CaSR_Variables"]["wind_direction"]
        dir_da = dir_ds[dir_var]

        spd_ds = xr.open_dataset(spd_path, chunks={})
        spd_var = casr_vars["CaSR_Variables"]["wind_speed"]
        spd_da = spd_ds[spd_var]

        # find neighbours on first file (assume grid is consistent across files)
        if ii == 0:
            neighbours = nearest_points(spd_da['lat'], spd_da['lon'], lat, lon, k=4)
            # initialize neighbor_series entries
            for n in neighbours:
                neighbor_series[n['flat_index']] = {
                    'idx': n['flat_index'], 'lat': n['lat'], 'lon': n['lon'], 'ij': n['ij'], 'distance': n['distance_deg'],
                    't0': [], 'dates': [], 'speed': [], "direction": []
                }
                neighbor_order = [n['flat_index'] for n in neighbours]

        # For each neighbour, extract the timeseries from spd_da at the ij position
        for flat_idx in neighbor_order:
            entry = neighbor_series[flat_idx] 
            i, j = entry['ij']
            # select by position; handle different dim names 

            try:
                s_sel = spd_da.isel(lat=int(i), lon=int(j))
                d_sel = dir_da.isel(lat=int(i), lon=int(j))

            except Exception:
                try:
                    s_sel = spd_da.isel({ 'lat': int(i), 'lon': int(j) })
                    d_sel = dir_da.isel({ 'lat': int(i), 'lon': int(j) })
                except Exception:
                    s_sel = spd_da.isel({list(spd_da.dims)[-2]: int(i), list(spd_da.dims)[-1]: int(j) })
                    d_sel = dir_da.isel({list(dir_da.dims)[-2]: int(i), list(dir_da.dims)[-1]: int(j) })

            # compute small selection if Dask-backed
            if hasattr(s_sel.data, 'compute'):
                s_sel = s_sel.compute()
                d_sel = d_sel.compute()
            
            # pull times
            s_vals = []
            d_vals = []
            times_np = s_sel['time'].values
            # convert numpy datetime64 to python ISO strings using pandas
            times_str = _pd.to_datetime(times_np).strftime('%Y-%m-%dT%H:%M:%S').tolist()
            s_vals = s_sel.values.ravel().tolist()
            d_vals = d_sel.values.ravel().tolist()

            # append to cumulative lists
            entry['dates'] = list(times_str)
            entry['t0'] = list(times_str)[0]
            entry['speed'] = ([ (float(v) if (v is not None and not np.isnan(v)) else None) for v in s_vals ])
            entry['direction'] = ([ (float(v) if (v is not None and not np.isnan(v)) else None) for v in d_vals ])

            # add everything to dataframe
            results.loc[len(results)] = entry

        # close dataset to free up some resources
        try:
            spd_ds.close()
        except Exception:
            pass

    return results
