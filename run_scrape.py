# -*- coding: utf-8 -*-
"""
    updater.py
    ~~~~~~~~~~

    Downloads raw data from Google Drive and converts it into a long CSV format
    suitable for plotting.
"""

import argparse
import os
import json
import pandas as pd
import numpy as np
from weatherlink.importer import Importer
from weatherlink.models import convert_timestamp_to_datetime
from weatherlink.utils import convert_fahrenheit_to_celsius, convert_miles_per_hour_to_meters_per_second

LOCAL_DIR = "raw_data"
AQ_DIR = os.path.join(LOCAL_DIR, "AQ")
MET_DIR = os.path.join(LOCAL_DIR, "MET")
COLUMNS_FN = "fields.json"


def main():
    args = parse_args()

    # Load field mapping dictionary
    try:
        with open(COLUMNS_FN, "r", encoding='utf8') as infile:
            field_names = json.load(infile)
    except FileNotFoundError:
        print(
            "Error: cannot open required file '{}', terminating execution.".format(
                COLUMNS_FN
            )
        )
        return
    except json.decoder.JSONDecodeError:
        print(
            "Error: cannot parse {} as JSON, terminating execution.".format(COLUMNS_FN)
        )
        return

    try:
        met_fields = field_names["meteorological"]
        aq_fields = field_names["airquality"]
    except KeyError:
        print(
            "Error: {} must contain 'airquality' and 'meteorological objects.".format(
                COLUMNS_FN
            )
        )
        return

    # Load both datasets into long data frames
    met_data = load_dataset(
        load_met_file, met_fields, MET_DIR
    )
    aq_data = load_dataset(
        load_airquality_file, aq_fields, AQ_DIR
    )

    if met_data is None:
        print("Error: no clean meteorological data loaded, terminating execution.")
        return
    if aq_data is None:
        print("Error: no clean air quality data loaded, terminating execution.")
        return

    # Combine into a data frame, clean, and save to file
    combined = pd.merge(met_data, aq_data, on="timestamp", how="outer")
    combined = clean(combined)
    try:
        combined.to_csv(args.output, index=False)
        print("Cleaned data saved to {}.".format(args.output))
    except FileNotFoundError:
        print("Cannot save to {}.".format(args.output))


def parse_args():
    """
    Parses CLI arguments to the script.

    Args:
        - None

    Returns:
        An argparse.Namespace object.
    """
    parser = argparse.ArgumentParser(description="COZI data processing")
    parser.add_argument(
        "output",
        metavar="FILE",
        help="Specify the output filepath to save the processed CSV data to.",
    )

    args = parser.parse_args()
    return args


def convert_excel_time(excel_time):
    """
    converts excel float format to pandas datetime object
    round to '1min' with 
    .dt.round('1min') to correct floating point conversion innaccuracy
    
    Args:
        - excel_time (float): The excel timestamp (days since 30st Dec 1899

    Returns:
        pandas datetime object.
    """
    converted = pd.to_datetime("1899-12-30") + pd.to_timedelta(excel_time, "D")
    return converted.dt.round("S")


def load_dataset(load_function, fields, localdir):
    """
    Loads a dataset from disk into memory, combining all observations into a
    single long pandas.DataFrame.

    Args:
        - load_function (function): The function to use to load a data file from
            this dataset into Pandas. It needs to be parameterised to accept 2
            arguments:
                - filename
                - fields
            And must return a wide Pandas DataFrame with a timestamp column
        - fields (dict): Mapping between {raw_label: clean_label}, where
            raw_label is the column name in the raw data on Google Drive, and
            clean_label is the desired label for our output data.
        - localdir (string): Filepath to a temporary local directory where files
            can be downloaded to.

    Returns:
        A pandas.DataFrame object with 3 columns:
            - timestamp: In YYYY-mm-dd HH:MM:SS format
            - measurand: Name of measurand as human readable string
            - value: Measurement value as float.
    """
    # Load all data into a single data frame
    print(f"Processing data from {localdir} into single file...")
    dfs = []
    for fn in os.listdir(localdir):
        df = load_function(os.path.join(localdir, fn), fields)
        if df is None:
            continue

        dfs.append(df)

    # Combine all clean datasets into 1 frame and drop empty values
    if len(dfs) >= 1:
        combined = pd.concat(dfs)
        combined.dropna(inplace=True, how='all')
    else:
        combined = None

    return combined


def load_airquality_file(filename, fields):
    """
    Reads CSV file containing air quality data into memory.

    Subsets dataset into fields of interest and converts timestamp from Excel
    format into ISO 8061.

    Args:
        - filename (str): File to load.
        - fields (dict): Mapping between {raw_label: clean_label}, where
            raw_label is the column name in the raw data on Google Drive, and
            clean_label is the desired label for our output data.

    Returns:
        A pandas.DataFrame object with as many columns as there are entries in
        fields, with the columns set as the attributes if the read is successful,
        None otherwise.
    """
    try:
        df = pd.read_csv(filename, usecols=fields.keys(), header=0)
    except pd.errors.EmptyDataError:
        print("{} is empty, skipping contents.".format(filename))
        return None
    except (pd.errors.ParserError, ValueError) as ex:
        print("Unable to parse {} as CSV, skipping contents.".format(filename))
        return None

    # Rename columns to have the specified labels
    df = df.rename(columns=fields)
    # Convert Excel timestamp into ISO
    df["timestamp"] = convert_excel_time(df["timestamp"])

    return df


def load_met_file(filename, fields):
    """
    Reads CSV file containing meteorological data into memory.

    Subsets dataset into fields of interest and converts 2 separate date and
    time columns into ISO 8061 timestamp.

    Args:
        - filename (str): File to load.
        - fields (dict): Mapping between {raw_label: clean_label}, where
            raw_label is the column name in the raw data on Google Drive, and
            clean_label is the desired label for our output data.

    Returns:
        A pandas.DataFrame object with as many columns as there are entries in
        fields, with the columns set as the attributes if the read is successful,
        None otherwise.
    """
    try:
        importer = Importer(filename)
        importer.import_data()
    except FileNotFoundError:
        print("Cannot find file {}.".format(filename))
        return None
    except ValueError:
        print("Cannot read file {}.".format(filename))
        return None

    # Limit fields to those required
    clean_fields = [{field: row[field] for field in fields.keys()} for row in importer.records]

    # Convert fields to ISO8061/metric. These weatherlink conversion functions
    # aren't vectorised
    for record in clean_fields:
        if record['timestamp'] is not None:
            record['timestamp'] = convert_timestamp_to_datetime(record['timestamp'])
        if record['temperature_outside'] is not None:
            record['temperature_outside'] = convert_fahrenheit_to_celsius(record['temperature_outside'])
        if record['wind_speed'] is not None:
            record['wind_speed'] = convert_miles_per_hour_to_meters_per_second(record['wind_speed'])

    # Parse list of dicts as pandas data frame
    try:
        df = pd.DataFrame(clean_fields)
    except pd.errors.EmptyDataError:
        print("{} is empty, skipping contents.".format(filename))
        return None
    except (pd.errors.ParserError, ValueError, KeyError) as ex:
        print("Unable to parse {} as CSV, skipping contents.".format(filename))
        return None

    # Rename columns to have the specified labels
    df = df.rename(columns=fields)

    return df


def clean(df):
    """
    Cleans the final data frame.

    Currently this just removes values outside of hardcoded limits.

    Args:
        - df (pd.DataFrame): The input data frame.

    Returns:
        A pd.DataFrame.
    """
    thresholds = {
        "Temperature (°C)": {
            "lower": -1000,
            "upper": np.Inf
        },
        "Relative humidity (%)": {
            "lower": 0,
            "upper": np.Inf
        },
        "NO (ppbV)": {
            "lower": 0,
            "upper": 200
        },
        "NO2 (ppbV)": {
            "lower": 0,
            "upper": 200
        },
        "NOx (ppbV)": {
            "lower": 0,
            "upper": 200
        },
        "CO (ppbV)": {
            "lower": 0,
            "upper": 400
        },
        "CO2 (ppmV)": {
            "lower": 0,
            "upper": 550
        },
        "CH4 (ppmV)": {
            "lower": 0,
            "upper": 100
        }
    }
    for col in thresholds:
        df.loc[df[col] <= thresholds[col]["lower"], col] = np.NaN
        df.loc[df[col] >= thresholds[col]["upper"], col] = np.NaN

    # Set all columns except timestamp as float
    types = {k: 'float64' for k in df.columns}
    types['timestamp'] = 'datetime64[ns]'
    df = df.astype(types)

    # Resample to 1 minute average
    df = df.set_index('timestamp').resample("1 Min").mean().reset_index()

    # Remove March 2021 CH4 and C0
    df.loc[(df['timestamp'] >= "2021-02-15") & (df['timestamp'] < "2021-04-01"), ["CH4 (ppmV)", "CO2 (ppmV)"]] = np.nan

    return df


if __name__ == "__main__":
    main()
