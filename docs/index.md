---
title: ESGF Virtual Zarr Data Access
---

# ESGF Virtual Zarr Data Access

An ESGF working group effort to enable streaming access to CMIP6 climate data via [virtual Zarr stores](https://zarr.dev), served through official ESGF channels alongside the existing NetCDF download pathway.

## Motivation

Current ESGF data access requires downloading large NetCDF files before analysis. Virtual Zarr stores allow users to stream data directly from ESGF servers using any Zarr-compatible client — without duplicating data and without waiting for downloads.

This project builds on the experience of the [Pangeo / ESGF Cloud Data Working Group](https://pangeo-data.github.io/pangeo-cmip6-cloud/) and aims to:

- Enable streaming access without data duplication
- Serve virtual references through official ESGF channels (alongside existing NetCDF files)
- Support [CMIP6+](https://wcrp-cmip.org/cmip6plus/) and [CMIP7](https://wcrp-cmip.org/cmip7/)

## Approach

We use [VirtualiZarr](https://github.com/zarr-developers/VirtualiZarr) to create lightweight reference files that point to byte ranges inside existing NetCDF files on ESGF servers. These references are stored in [Icechunk](https://github.com/earth-mover/icechunk) repositories, enabling Zarr V3 access with no data movement.

We prefer VirtualiZarr over Kerchunk because it:
- Is backwards-compatible with the Kerchunk format
- Supports native Zarr V3 output via Icechunk
- Provides an xarray-native API for combining references

## Milestones

- [x] First working [proof of concept](notebooks/virtualizarr_V2_example.ipynb) for virtual Zarr stores pointing to ESGF servers
- [ ] Prototype integration of reference file generation into the [ESGF publishing pipeline](https://github.com/ESGF/esg-publisher)

## References

- [ESGF Roadmap Issue #5](https://github.com/ESGF/esgf-roadmap/issues/5) — motivation for streaming access
- [Tenth ESGF Hybrid Conference](https://drive.google.com/file/d/1A43T3iz_49y5xta4ssBaacyqfiwNMNtO/view) — context for this work
