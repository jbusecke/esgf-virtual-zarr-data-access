# esgf-virtual-zarr-data-access
ESGF working group to enable data access via virtual zarrs.

## Motivation
We aim to establish streaming access via [zarr](https://zarr.dev) as a officially supported access pattern next to local downloading. https://github.com/ESGF/esgf-roadmap/issues/5 provides more justification.

This effort draws heavily from the experience of the [Pangeo / ESGF Cloud Data Working Group](https://pangeo-data.github.io/pangeo-cmip6-cloud/)
We aim to do this:
- Without duplicating most/all of the data
- Serving this via official ESGF channels (next to netcdf files).

## Guide

1. Install the required dependencies via pip
```
mamba create -n esgf-virtual-zarr-data-access python=3.11
mamba activate esgf-virtual-zarr-data-access
pip install -r requirements.txt
```

2. Modify the urls, and the output json filename in `virtual-zarr-script.py`, and run the script.
```
python virtual-zarr-script.py
``` 

3. Check that the generated JSON file is readable with xarray and average the full dataset (this is also done in the script)

```python
import xarray as xr
ds = xr.open_dataset(
    '<your_filename>.json', 
    engine='kerchunk',
    chunks={},
)
ds.mean().load() # test that all chunks can be accessed.
```

## Goals

On the [Tenth Earth System Grid Federation (ESGF) Hybrid Conference](https://drive.google.com/file/d/1A43T3iz_49y5xta4ssBaacyqfiwNMNtO/view) we discussed the option to serve virtualized zarr files (kerchunk reference files for demonstration's sake). We saw an excellent demo by @rhysrevans3 who showed how to serve both the virtual zarr and the individual netcdf files as a [STAC catalog](https://stacspec.org/en). 

- Our goal here is to show the feasability of streaming access to CMIP data via ESGF infrastructure for [CMIP6+](https://wcrp-cmip.org/cmip6plus/) and beyond ([CMIP7](https://wcrp-cmip.org/cmip7/))

## Milestones
- [ ] Demonstrate a proof of concept which shows that we can create virtual zarr stores, which point to data on ESGF servers, that work functionally equivalent to loading a native zarr from cloud storage.
    - First working [prototype](https://github.com/jbusecke/esgf-virtual-zarr-data-access/blob/main/notebooks/proof-of-concept.ipynb)
- [ ] Prototype how the generation of reference files is integrated into the future [ESGF publishing](https://github.com/ESGF/esg-publisher)

## Examples:

```
import xarray as xr
from dask.diagnostics import ProgressBar
DSID="CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.pr.gn.v20190401"
esgf_url = f"http://esgf-data4.llnl.gov/thredds/fileServer/user_pub_work/vzarr/{DSID}.json"
ds = xr.open_dataset(
    esgf_url, 
    engine='kerchunk',
    chunks={},
)
with ProgressBar():
    a = ds.mean().load()
```


### Why not Kerchunk?
- I (@jbusecke) strongly suggest to use [Virtualizarr](https://github.com/TomNicholas/VirtualiZarr) to future proof the development here:
  - Is currently backwards compatible with kerchunk.
  - Save out native Zarr. This will enable users to use any Zarr client library to read the data (needs V3?)
  - Reference creation in xarray native, enables cleaner API.
 
**[Open Questions](https://github.com/jbusecke/esgf-virtual-zarr-data-access/labels/question)**

**[Upstream Requirements](https://github.com/jbusecke/esgf-virtual-zarr-data-access/labels/upstream)**
