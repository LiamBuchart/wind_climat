# Disclaimer

Simp istructions to download CaSR Reanalysis data provided by ECCC.

A file for a wget command that you should copy into your terminal to download the casr wind data
Anothing fancy just faster to do it this way rather than a python script

## Notation

Link: `https://hpfx.collab.science.gc.ca/~scar700/rcas-casr/data/CaSRv3.1/netcdf/`

File name notation is: `YYYYMMDD12.nc`. Each wget call will have to modify the year and day.

Since I am building a climatology, I will download and process data one year at a time, thus a simpe wget command such as:

wget https://hpfx.collab.science.gc.ca/~scar700/rcas-casr/data/CaSRv3.1/netcdf/YYYY* -r 

will suffice. I then get averaged of the desired wind variables for each month and season.

Note: I also need the december data from the previous year to get the meteorological seasons. To get this month worth of data 

wget https://hpfx.collab.science.gc.ca/~scar700/rcas-casr/data/CaSRv3.1/netcdf/YYYY(-1)12* -r

Then you can process you data. Once done, I remove all of the .nc files from CaSR (leaving just the December data) so I dont bog down my machine. 
