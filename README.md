# Convert SNANA FITS light curves to parquet

`./fits_to_parquet.py` transforms data from SNANA format to parquet and optionally filter the data.
See `--help` for options.

### Convert Elasticc 2 train dataset

- Download the data with [this link](https://portal.nersc.gov/cfs/lsst/DESC_TD_PUBLIC/ELASTICC/ELASTICC2_TRAINING_SAMPLE_2/ELASTICC2_TRAIN_02.tar.bz2)
- Unpack it to `data/data/elasticc2_train/raw`
- `cd data/data/elasticc2_train`
- Convert all the data to parquet: `ls raw | sed 's/ELASTICC2_TRAIN_02_//' | xargs -IXXX -P32 ../../fits_to_parquet.py raw/ELASTICC2_TRAIN_02_XXX parquet/XXX.parquet`
- Or applying some filtering: `ls raw | sed 's/ELASTICC2_TRAIN_02_//' | xargs -IXXX -P32 ../../fits_to_parquet.py raw/ELASTICC2_TRAIN_02_XXX parquet_r_min5obs_photflags/XXX.parquet --band=r --filter-by-photflag --min-nobs=5`


### Column information

The main light curve columns are:
- **MJD** - date in days
- **FLUXCAL** - flux in some weird units
- **FLUXCALERR** - flux error
- **BAND** - passband, one of *ugrizY*

Useful meta-information columns:
- **SNID** - id of the object, could be non-unique within different individual parquet files
