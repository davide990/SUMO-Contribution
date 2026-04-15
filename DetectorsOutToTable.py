#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Eclipse SUMO, Simulation of Urban MObility; see https://eclipse.dev/sumo
# Copyright (C) 2013-2025 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# This Source Code may also be made available under the following Secondary
# Licenses when the conditions for such availability set forth in the Eclipse
# Public License 2.0 are satisfied: GNU General Public License, version 2
# or later which is available at
# https://www.gnu.org/licenses/old-licenses/gpl-2.0-standalone.html
# SPDX-License-Identifier: EPL-2.0 OR GPL-2.0-or-later

# @file    DetectorsOutToTable.py
# @author  Davide Guastella
# @date    2025-056-10

"""
    Convert a detector output file (generated from induction loops) to a table where rows are the time instants, 
    columns are the pivot value provided in input.

    Example:
        python DetectorsOutToTable.py -a det.add.xml -d det.out.xml -p interval_entered -o det_table.csv
"""

import os
from pathlib import Path
import pandas as pd
import sumolib
import argparse
import sys
import polars as pl

if 'SUMO_HOME' in os.environ:
    sys.path.append(os.path.join(os.environ['SUMO_HOME'], 'tools', 'xml'))
    import xml2csv
else:
    sys.exit("Please declare environment variable 'SUMO_HOME'")

def det_out_to_table(fname, edges_names, pivot_attribute="interval_nVehContrib"):
    sim_data_pl = pl.from_pandas(pd.read_csv(fname, sep=";"))

    sim_data_df_formatted = pd.DataFrame()
    for real_edge_name in edges_names:
        aggr_counts = sim_data_pl.filter(pl.col('interval_id').str.contains(real_edge_name)).group_by(
            ["interval_begin"]).agg(pl.col(pivot_attribute).sum()).to_pandas(use_pyarrow_extension_array=False)
        aggr_counts['interval_id'] = real_edge_name
        sim_data_df_formatted = pd.concat([sim_data_df_formatted, aggr_counts])

    sim_data_df_formatted.reset_index(inplace=True, drop=True)
    sim_data_df_formatted.sort_values(by=['interval_begin'], inplace=True)

    return sim_data_df_formatted


def main():
    parser = argparse.ArgumentParser(description="Convert the output from detectors (in xml) in table format (CSV)")
    parser.add_argument("-a", "--detectors_def", help="Input detectors definition file", required=True)
    parser.add_argument("-d", "--detectors_out", help="Output detector file", required=True)
    parser.add_argument("-p", "--pivot_parameter", required=True, help="The pivot parameter (the content of the table)")
    parser.add_argument("-o", "--output", required=True, help="The path to the output CSV file")

    args = parser.parse_args()
    det_add_xml = args.detectors_def
    det_out_virt_xml = args.detectors_out
    pivot_attribute = args.pivot_parameter

    filename = Path(det_out_virt_xml)
    det_out_virt_csv = str(filename.with_suffix('.csv'))
    sensors_lanes_mapping = dict(sumolib.xml.parse_fast(det_add_xml, 'inductionLoop', ['id', 'lane']))
    xml2csv.main([det_out_virt_xml, '-o', det_out_virt_csv])
    df = det_out_to_table(det_out_virt_csv, list(sensors_lanes_mapping.keys()), args.pivot_parameter)
    pivot_df = df.pivot_table(index='interval_begin', columns='interval_id', values=pivot_attribute)
    pivot_df.to_csv(args.output, sep=";")


if __name__ == '__main__':
    main()
