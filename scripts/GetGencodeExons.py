#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2023 Ryan L. Collins and the Van Allen/Gusev/Haigis Laboratories
# Distributed under terms of the GPL-2.0 License (see LICENSE)
# Contact: Ryan L. Collins <Ryan_Collins@dfci.harvard.edu>

"""
Extract exons from a .gtf
"""


import argparse
import pybedtools as pbt


def main():
    """
    Main block
    """
    parser = argparse.ArgumentParser(
             description=__doc__,
             formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('gtf_in', help='input gtf')
    parser.add_argument('bed_out', help='output BED')
    parser.add_argument('--genome', help='BEDTools-style genome file (optional, ' +
                        'used for sorting output)')
    args = parser.parse_args()

    ex_bt = pbt.BedTool(args.gtf_in).filter(lambda x: x.fields[2] == 'exon')

    if args.genome is not None:
        ex_bt.sort(g=args.genome).merge().saveas(args.bed_out)
    else:
        ex_bt.sort().merge().saveas(args.bed_out)


if __name__ == '__main__':
    main()

