# esgf-virtual-zarr-data-access
ESGF working group to enable data access via virtual zarrs.

## Goals

On the [Tenth Earth System Grid Federation (ESGF) Hybrid Conference](https://drive.google.com/file/d/1A43T3iz_49y5xta4ssBaacyqfiwNMNtO/view) we discussed the option to serve virtualized zarr files (kerchunk reference files for demonstration's sake). We saw an excellent demo by @rhysrevans3 who showed how to serve both the virtual zarr and the individual netcdf files as a [STAC catalog](https://stacspec.org/en). 

- Our goal here is to show the feasability of streaming access to CMIP data via ESGF infrastructure for [CMIP6+](https://wcrp-cmip.org/cmip6plus/) and beyond ([CMIP7](https://wcrp-cmip.org/cmip7/))

## Milestones
- [ ] Demonstrate a proof of concept which shows that we can create virtual zarr stores, which point to data on ESGF servers, that work functionally equivalent to loading a native zarr from cloud storage.
- [ ] Prototype how the generation of reference files is integrated into the future [ESGF publishing](https://github.com/ESGF/esg-publisher)


### Why not Kerchunk?
- I (@jbusecke) strongly suggest to use [Virtualizarr](https://github.com/TomNicholas/VirtualiZarr) to future proof the development here:
  - Is currently backwards compatible with kerchunk.
  - Save out native Zarr. This will enable users to use any Zarr client library to read the data (needs V3?)
  - Reference creation in xarray native, enables cleaner API.
 
**[Open Questions](https://github.com/jbusecke/esgf-virtual-zarr-data-access/labels/question)**

**[Upstream Requirements](https://github.com/jbusecke/esgf-virtual-zarr-data-access/labels/upstream)**
