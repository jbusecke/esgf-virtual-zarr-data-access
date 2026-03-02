# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Dependencies
```bash
uv sync                   # install all project dependencies
uv sync --group docs      # also install mystmd for docs
```

### Run scripts
```bash
uv run python virtual-zarr-script.py
uv run python reference_generation/netcdf4_s3_icechunk.py
```

### Linting
```bash
uv run ruff check --fix
uv run ruff format
```

### Docs (requires `uv sync --group docs` first)
```bash
myst start          # local preview server at http://localhost:3000
myst build --html   # build static HTML to ./_build/html/
```

### Jupyter kernel
```bash
uv run python -m ipykernel install --user --name=esgf-virtual-zarr
jupyter kernelspec uninstall esgf-virtual-zarr
```

## Architecture

This is a research project with no installable Python package — it contains scripts and notebooks demonstrating how to create virtual Zarr reference stores for CMIP6 data.

**Two reference-generation patterns:**

1. **HTTP + kerchunk JSON** (`virtual-zarr-script.py`): Opens NetCDF files from ESGF Thredds HTTP URLs as VirtualiZarr virtual datasets, concatenates along time, writes kerchunk JSON. Simple and serial.

2. **S3 + dask + icechunk** (`reference_generation/netcdf4_s3_icechunk.py`): Opens NetCDF from `s3://esgf-world/`, parallelises with dask, writes to kerchunk parquet then converts to an [IcechunkStore](https://github.com/earth-mover/icechunk) on S3 for Zarr V3 access.

**Key libraries:**
- `virtualizarr` — installed from git HEAD (not PyPI); entry points: `open_virtual_dataset`, `open_virtual_mfdataset`, `HDFParser`, `ObjectStoreRegistry`
- `icechunk` — Zarr V3 virtual reference store with `StorageConfig`, `StoreConfig`, `VirtualRefConfig`
- `kerchunk` — legacy format support (JSON/parquet)

**Docs:** MyST-MD site with source in `docs/` and `notebooks/`. Config in `myst.yml`. Deployed to GitHub Pages via `.github/workflows/deploy-docs.yml` on push to `main`. Notebooks are rendered statically (not re-executed in CI).

**Output directories:**
- `refs/` — intermediate and final reference files (parquet, icechunk repo)
- `./_build/html/` — built docs (gitignored)
