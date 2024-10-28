

# dask distributed icechunk s3fs git+https://github.com/mpiannucci/kerchunk@v3 https://github.com/zarr-developers/numcodecs@zarr3-codecs
# TMP BRANCH https://github.com/zarr-developers/VirtualiZarr/pull/278


# To do: We can generalize all these funcs and just have input filelists OR subsets of intake catalogs
# https://pangeo-data.github.io/pangeo-cmip6-cloud/accessing_data.html#manually-searching-the-catalog

# To Do:
# For now, we are just doing a time concat, but we can merge across vars later

import xarray as xr
from virtualizarr import open_virtual_dataset
import dask
from dask.distributed import Client
import pandas as pd


client = Client(n_workers=16)
client

urls = [
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_185001-186012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_187101-188012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_188101-189012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_186101-187012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_189101-190012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_190101-191012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_191101-192012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_192101-193012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_193101-194012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_194101-195012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_195101-196012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_196101-197012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_197101-198012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_198101-199012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_199101-200012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_200101-201012.nc',
    's3://esgf-world/CMIP6/CMIP/CCCma/CanESM5/historical/r10i1p1f1/Omon/uo/gn/v20190429/uo_Omon_CanESM5_historical_r10i1p1f1_gn_201101-201412.nc'
]


vds = open_virtual_dataset(urls[0], indexes={})

def process(filename):
    vds = open_virtual_dataset(filename,  indexes={})
    return vds


delayed_results = [dask.delayed(process)(filename) for filename in urls]

# compute delayed obs
results = dask.compute(*delayed_results)

# concat virtual datasets
combined_vds = xr.concat(list(results), dim="time", coords="minimal", compat="override")

# once icechunk PR is in, we can write to Zarr v3
# for now, we write to parquet, then will RT to icechunk with 'icechunk' env
ref_parquet_path = "../refs/netcdf4_s3_icechunk.parquet"
combined_vds.virtualize.to_kerchunk(
    ref_parquet_path, format="parquet"
)



###### WRITE ICECHUNK - THIS REQUIRES envs/icechunk_env.yaml #####

# 1. read existing ref

vds = open_virtual_dataset('netcdf4_s3_icechunk.parquet',filetype = 'kerchunk', indexes={})

# create an icechunk store
from icechunk import IcechunkStore, StorageConfig, StoreConfig, VirtualRefConfig
import icechunk 

storage_config = icechunk.StorageConfig.filesystem("./netcdf4_s3_icechunk")
store = icechunk.IcechunkStore.create(storage_config)

# write to icechunk store
vds.virtualize.to_icechunk(store)
store.commit("init")




