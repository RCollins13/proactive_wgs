#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2023 Ryan L. Collins and the Van Allen/Gusev/Haigis Laboratories
# Distributed under terms of the GPL-2.0 License (see LICENSE)
# Contact: Ryan L. Collins <Ryan_Collins@dfci.harvard.edu>

"""
Extract sample information from gnomAD metadata for HGDP + 1000G samples
"""


import argparse
import pandas as pd

# Hail helpers
false = False
true = True
null = None


def extract_relatives(rel_dict):
    """
    Convert a single entry in df.relatedness_inference to a string of relatives
    """

    return ','.join(sorted([m['s'] for m in eval(rel_dict).get('related_samples')]))


def get_useful_metadata(meta_dict):
    """
    Extract various useful metadata tidbits for a single sample
    """

    keys = 'project genetic_region gnomad_labeled_subpop'.split()
    return [eval(meta_dict)[k] for k in keys]


def main():
    """
    Main block
    """
    parser = argparse.ArgumentParser(
             description=__doc__,
             formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('tsv_in', help='input metadata .tsv')
    parser.add_argument('tsv_out', help='output metadata .tsv')
    args = parser.parse_args()

    df = pd.read_csv(args.tsv_in, sep='\t')

    # Parse related samples
    df['relatives'] = df.relatedness_inference.apply(extract_relatives)

    # Parse other useful metadata
    meta_df = df.hgdp_tgp_meta.apply(get_useful_metadata)
    meta_df = pd.DataFrame(meta_df.to_list(), columns='cohort pop subpop'.split())
    df = pd.concat([df, meta_df], axis=1)

    # Subset to samples from 1000G or HGDP
    df = df[df.cohort.isin(['HGDP', '1000 Genomes'])]

    # Reformat output and save to args.tsv_out
    df.rename(columns={'s' : '#sample_id'}, inplace=True)
    df = df['#sample_id cohort pop subpop relatives'.split()].\
             sort_values(by='pop subpop #sample_id'.split())
    df.to_csv(args.tsv_out, sep='\t', index=False)
    

if __name__ == '__main__':
    main()

