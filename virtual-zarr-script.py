from tqdm.auto import tqdm
from virtualizarr import open_virtual_dataset
import xarray as xr
from dask.diagnostics import ProgressBar

urls = [
    "http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_201501-201912.nc",
    "http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_202001-202412.nc",
    "http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_202501-202912.nc",
    "http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_203001-203412.nc",
]
json_filename = "combined_full.json"


# load virtual datasets in serial
vds_list = []
for url in tqdm(urls):
    vds = open_virtual_dataset(
        url, indexes={}, 
        # reader_options={}
    )  # reader_options={} is needed for now to circumvent a bug in https://github.com/TomNicholas/VirtualiZarr/pull/126
    vds_list.append(vds)

combined_vds = xr.combine_nested(
    vds_list,
    concat_dim=["time"],
    coords="minimal",
    compat="override",
    combine_attrs="drop_conflicts",
)
combined_vds.virtualize.to_kerchunk(json_filename, format="json")

## load and testthe output
print(f"Loading the mean of the virtual dataset from {json_filename=}")

ds = xr.open_dataset(
    json_filename, 
    engine='kerchunk',
    chunks={},
)
print(f"Dataset before mean: {ds}")
with ProgressBar():
    ds_mean = ds.mean().load()
print(ds_mean)

print("Checking time decoding")



