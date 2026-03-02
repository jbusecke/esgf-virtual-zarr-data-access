---
title: Getting Started
---

# Getting Started

## Installation

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync
```

## Running the Example Script

Modify the URLs and output filename in `virtual-zarr-script.py`, then run:

```bash
uv run python virtual-zarr-script.py
```

## Reading a Virtual Zarr Store

Once a reference file has been generated, open it with xarray using the `kerchunk` engine:

```python
import xarray as xr

ds = xr.open_dataset(
    "combined_full.json",
    engine="kerchunk",
    chunks={},
)
ds.mean().load()  # streams data directly from ESGF servers
```

Or read from a hosted reference file:

```python
import xarray as xr
from dask.diagnostics import ProgressBar

DSID = "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.pr.gn.v20190401"
esgf_url = f"http://esgf-data4.llnl.gov/thredds/fileServer/user_pub_work/vzarr/{DSID}.json"

ds = xr.open_dataset(esgf_url, engine="kerchunk", chunks={})
with ProgressBar():
    a = ds.mean().load()
```

## Using Jupyter

Register the project environment as a Jupyter kernel:

```bash
uv run python -m ipykernel install --user --name=esgf-virtual-zarr
```

Then launch Jupyter Lab or Notebook and select the `esgf-virtual-zarr` kernel.

To remove the kernel when done:

```bash
jupyter kernelspec uninstall esgf-virtual-zarr
```
