"""

    Main data processing automation script:
    Call the download script, then process, 
    Then clean the temp directory once each iteration is done

"""
#%%
import os
import numpy as np

from gen_hrly_winds import gen_hrly_files
from get_daily_CaSR import download_data, run_parallel
from utils import get_date_from_years, get_days_in_month

link = "https://hpfx.collab.science.gc.ca/~scar700/rcas-casr/data/CaSRv3.1/netcdf/"
# Set date range

yearS = 1989
yearE = 2019
months = np.arange(12, 12+1)  # exclude december since it will need a slightly diffrent start year
save_dir = "./climatology/daily/"

# Path to temp folder
temp_dir = './temp/'
for month in months:
    days = get_days_in_month(2019, int(month)) # just didnt want a leap year
    for day in days:
        dates = get_date_from_years(yearS, yearE, int(month), int(day))

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
            print(f"Beginning to process for {month}, {day}...")
            run_parallel(dates, link)

            print("Now processing netcdf files to extract winds...")
            gen_hrly_files(month, day)

        # Clean .nc files from temp folder
        print("Cleaning temp directory...")
        for fname in os.listdir(temp_dir):
            if fname.endswith('.nc'):
                # Attempt to remove the .nc file. On Windows this can fail with
                # PermissionError if another process still has the file open
                # (common with netCDF/h5 files if a dataset wasn't closed).
                file_path = os.path.join(temp_dir, fname)
                try:
                    os.remove(file_path)  # unlink
                except PermissionError as e:
                    # Informative message and skip – the file is in use.
                    print(f"Could not remove '{file_path}' (in use): {e}. Skipping.")
                except FileNotFoundError:
                    # Already removed by another thread/process; ignore
                    pass
                except Exception as e:
                    # Unexpected error – report and continue
                    print(f"Error removing '{file_path}': {e}")
        print(f"Cleaned .nc files from {temp_dir}")

    print(f"All dates in {month:02d} are processed.")

print("All dates processed.")
# %%
