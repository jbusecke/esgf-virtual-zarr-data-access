import pytest
import xarray as xr
import fsspec
from virtualizarr import open_virtual_dataset

urls = [
    "http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_201501-201912.nc",
    "http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_202001-202412.nc",
    # "http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_202501-202912.nc",
    # "http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp126/r1i1p1f1/Amon/tas/gn/v20190710/tas_Amon_MPI-ESM1-2-HR_ssp126_r1i1p1f1_gn_203001-203412.nc",
]

@pytest.fixture()
def ds_combined():
    ds_list = []
    for url in urls:
        with fsspec.open(url) as f:
            ds = xr.open_dataset(f, use_cftime=True).load() #workaround from https://github.com/fsspec/s3fs/issues/337
        ds_list.append(ds)
    return xr.combine_nested(
        ds_list,
        concat_dim=["time"],
        coords="minimal",
        compat="override",
        combine_attrs="drop_conflicts",
    )

@pytest.fixture(scope="module")
def vds():
    # load virtual datasets in serial
    vds_list = []
    for url in urls:
        vds = open_virtual_dataset(
            url, indexes={}, 
            reader_options={}, # needed for now to circumvent a bug in https://github.com/TomNicholas/VirtualiZarr/pull/126
            cftime_variables=["time"],
            loadable_variables=["time"],
        )
        vds_list.append(vds)

    combined_vds = xr.combine_nested(
        vds_list,
        concat_dim=["time"],
        coords="minimal",
        compat="override",
        combine_attrs="drop_conflicts",
    )
    return combined_vds

@pytest.fixture(scope="module")
def vds_json(vds, tmpdir_factory):
    json_filename = str(tmpdir_factory.mktemp('data').join("combined_full.json"))
    vds.virtualize.to_kerchunk(json_filename, format="json")
    return json_filename

def ds_from_json(json_filename, **kwargs):
    return xr.open_dataset(
        json_filename, 
        engine='kerchunk',
        **kwargs
    )

def test_time(vds_json, ds_combined, ):
    ds = ds_from_json(vds_json, chunks={}, use_cftime=True)
    def clean_time(ds: xr.Dataset) -> xr.DataArray:
        return ds.time.reset_coords(drop=True).load()
    xr.testing.assert_identical(clean_time(ds), clean_time(ds_combined))

@pytest.mark.parametrize('chunks', [None, {}])
def test_equal(vds_json, ds_combined, chunks):
    ds = ds_from_json(vds_json, chunks=chunks, use_cftime=True)
    ds = ds.load()
    ds_combined = ds_combined.load()
    print(f"{ds=}")
    print(f"{ds_combined=}")
    xr.testing.assert_identical(ds, ds_combined)