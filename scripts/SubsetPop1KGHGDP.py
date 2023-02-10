#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2023 Ryan L. Collins and the Van Allen/Gusev/Haigis Laboratories
# Distributed under terms of the GPL-2.0 License (see LICENSE)
# Contact: Ryan L. Collins <Ryan_Collins@dfci.harvard.edu>

"""
Subset 1000G + HGDP metadata to a list of unrelated samples from a single population
"""


import argparse
import pandas as pd


def count_relatives(df):
    """
    Gather count of relatives remaining in df
    """

    return df.relatives.dropna().explode().value_counts()


def update_relatives(rel, drop_samples=[]):
    """
    Update df.relatives values after dropping drop_samples
    """

    if type(rel) == list:
        rel = [s for s in rel if str(s) not in drop_samples]

    return rel


def main():
    """
    Main block
    """
    parser = argparse.ArgumentParser(
             description=__doc__,
             formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('meta_in', help='input metadata .tsv')
    parser.add_argument('txt_out', help='output sample list')
    parser.add_argument('--pop', required=True, help='population code')
    args = parser.parse_args()

    # Load data
    df = pd.read_csv(args.meta_in, sep='\t')
    df.rename(columns={'#sample_id' : 'sample'}, inplace=True)

    # Filter to population of interest
    df = df[df['pop'] == args.pop]
    print('Loaded {:,} samples from {} population\n'.format(len(df), args.pop))

    # Iteratively prune related samples to retain the maximum sample size
    df['relatives'] = df.loc[:, 'relatives'].str.split(',')
    relatives = count_relatives(df)
    for sample in relatives.index:

        # Drop sample from dataframe
        df = df[df['sample'] != sample]

        # Update relative list
        df['relatives'] = df.relatives.apply(update_relatives, drop_samples=[sample])
        relatives = count_relatives(df)

        # Finish when no related samples are left
        if len(relatives) == 0:
            break

    print('Retained {:,} samples after pruning relatives\n'.format(len(df), args.pop))

    # Write list of samples to output file
    df.to_csv(args.txt_out, columns=['sample'], header=False, index=False, sep='\t')

if __name__ == '__main__':
    main()

