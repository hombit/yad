#!/usr/bin/env python

from argparse import ArgumentParser
from itertools import chain
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
from sncosmo import read_snana_fits


def parse_args(argv=None):
    parser = ArgumentParser(
        prog='SNANA2parquet',
        description='Convert SNANA simulated light curves from FITS to parquet',
    )
    parser.add_argument('input', type=Path, 
                        help='Path to folder containing {HEAD,PHOT}.FITS.gz files')
    parser.add_argument('output', type=Path,
                        help='Output parquet file path, tipically has .parquet extension')
    parser.add_argument('--band', default=None,
                        help='Select a single passband from each light curve, default is to use keep all passbands')
    parser.add_argument('--filter-by-photflag', action='store_true',
                        help='Use PHOTFLAG to select detections (4096) and deselect saturations (1024)')
    parser.add_argument('--min-s2n', default=None, type=float,
                        help='Select points having at least the given number of detections')
    parser.add_argument('--min-nobs', type=int, default=0,
                        help='Select light curves having at least min_nobs observations. Applies after all other filters')

    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    head = sorted(args.input.glob('*HEAD.FITS.gz'))
    phot = sorted(args.input.glob('*PHOT.FITS.gz'))
    assert len(head) > 0
    assert len(head) == len(phot)

    lcs = list(chain.from_iterable(
        read_snana_fits(h, p) for h, p in zip(head, phot)
    ))

    # Fix band srtings - trim extra space
    for lc in lcs:
        lc['BAND'] = lc['BAND'].astype('U1')

    head_columns = list(lc.meta)
    phot_columns = list(lc.columns)

    df = pl.from_records(
        [
            lc.meta
            | {column: pl.Series(lc[column].newbyteorder().byteswap()) for column in lc.columns}
            for lc in lcs
        ],
    )
    count_lcs = len(lcs)
    del lcs

    # Make mandatory transformations
    df = df.with_columns(pl.col('SNID').cast(int))

    # Filtering
    df = df.lazy().explode(phot_columns)
    if args.band is not None:
        df = df.filter(pl.col('BAND') == args.band)
    if args.filter_by_photflag:
        df = df.filter((pl.col('PHOTFLAG') & 4096) != 0, (pl.col('PHOTFLAG') & 1024) == 0)
    if args.min_s2n is not None:
        df = df.filter(pl.col('FLUXCAL') / pl.col('FLUXCALERR') >= args.min_s2n)
    df = df.group_by('SNID').agg(phot_columns)
    if args.min_nobs > 0:
        df = df.filter(pl.col('MJD').list.len() >= args.min_nobs)

    df = df.collect()
    print(args.output, len(df), '/', count_lcs)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(args.output)

    return df, head_columns, phot_columns


if __name__ == '__main__':
    main()
