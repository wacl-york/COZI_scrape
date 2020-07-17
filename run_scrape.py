"""
    updater.py
    ~~~~~~~~~~

    Downloads raw data from Google Drive and converts it into a long CSV format
    suitable for plotting.
"""

import shutil
import argparse
import os
import io
import json
import pandas as pd
from google.oauth2 import service_account
import googleapiclient.discovery
from googleapiclient.http import MediaIoBaseDownload
from weatherlink.importer import Importer
from weatherlink.models import convert_timestamp_to_datetime
from weatherlink.utils import convert_fahrenheit_to_celsius, convert_miles_per_hour_to_meters_per_second

LOCAL_DIR = "tmp"
AQ_DIR = os.path.join(LOCAL_DIR, "AQ")
MET_DIR = os.path.join(LOCAL_DIR, "MET")
CREDENTIALS_FN = "credentials.json"
COLUMNS_FN = "fields.json"


def main():
    args = parse_args()

    # Create local directory to store data
    if os.path.exists(LOCAL_DIR):
        cleanup()
    os.makedirs(AQ_DIR)
    os.makedirs(MET_DIR)

    # Authenticate with Google Drive
    try:
        service = auth_google_api(CREDENTIALS_FN)
    except GoogleAPIError:
        print("Error connecting to service, terminating execution.")
        cleanup()
        return

    # Load field mapping dictionary
    try:
        with open(COLUMNS_FN, "r") as infile:
            field_names = json.load(infile)
    except FileNotFoundError:
        print(
            "Error: cannot open required file '{}', terminating execution.".format(
                COLUMNS_FN
            )
        )
        cleanup()
        return
    except json.decoder.JSONDecodeError:
        print(
            "Error: cannot parse {} as JSON, terminating execution.".format(COLUMNS_FN)
        )
        cleanup()
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
        cleanup()
        return

    # Load both datasets into long data frames
    met_data = load_dataset(
        service, "name contains '.wlk'", load_met_file, met_fields, MET_DIR
    )
    aq_data = load_dataset(
        service, "name contains 'logging'", load_airquality_file, aq_fields, AQ_DIR
    )

    if met_data is None:
        print("Error: no clean meteorological data loaded, terminating execution.")
        cleanup()
        return
    if aq_data is None:
        print("Error: no clean air quality data loaded, terminating execution.")
        cleanup()
        return

    # Combine into a data frame and save to file
    combined = pd.merge(met_data, aq_data, on="timestamp", how="outer")
    try:
        combined.to_csv(args.output, index=False)
        print("Cleaned data saved to {}.".format(args.output))
    except FileNotFoundError:
        print("Cannot save to {}.".format(args.output))
        cleanup()

    cleanup()


def cleanup():
    """
    Cleans up the working directory by removing temporary resources.

    Args:
        None.

    Returns:
        None, just deletes ancillary files.
    """
    shutil.rmtree(LOCAL_DIR)


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


class GoogleAPIError(Exception):
    """
    Custom exception class for errors related to using Google's API.
    """


def auth_google_api(filename):
    """
    Authorizes connection to GoogleDrive API.

    Requires GOOGLE_CREDS environment variable to be set, containing the
    contents of the JSON credential file, formatted as a string.

    Uses v3 of the GoogleDrive API, see examples at:
    https://developers.google.com/drive/api/v3/quickstart/python

    Args:
        - filename (str): Location of JSON containing credentials for service
        account.

    Returns:
        A googleapiclient.discovery.Resource object.
    """
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]

    try:
        credentials = service_account.Credentials.from_service_account_file(
            filename, scopes=scopes
        )
    except FileNotFoundError:
        raise GoogleAPIError(
            "Credential file '{}' not found".format(filename)
        ) from None
    except ValueError:
        raise GoogleAPIError("Credential file is not formatted as expected") from None

    # setting cache_discovery = False removes a large amount of warnings in log,
    # that seemingly have little performance impact as we don't need cache.
    service = googleapiclient.discovery.build(
        "drive", "v3", credentials=credentials, cache_discovery=False
    )
    return service


def load_dataset(service, query, load_function, fields, tempdir):
    """
    Loads a dataset that is stored on Google Drive into local memory.

    It queries the Google Drive API to find all the files from this dataset,
    downloads them all to a local temporary location, reads them into Pandas
    DataFrame objects, and converts them into a single long DataFrame.

    Args:
        - service (googleapiclient.discovery.Resource): A handle to a Google API
            service account.
        - query (string): The query string used to find the files to download
            from Google Drive. See https://developers.google.com/drive/api/v3/search-files
        - load_function (function): The function to use to load a data file from
            this dataset into Pandas. It needs to be parameterised to accept 2
            arguments:
                - filename
                - fields
            And must return a wide Pandas DataFrame with a timestamp column
        - fields (dict): Mapping between {raw_label: clean_label}, where
            raw_label is the column name in the raw data on Google Drive, and
            clean_label is the desired label for our output data.
        - tempdir (string): Filepath to a temporary local directory where files
            can be downloaded to.

    Returns:
        A pandas.DataFrame object with 3 columns:
            - timestamp: In YYYY-mm-dd HH:MM:SS format
            - measurand: Name of measurand as human readable string
            - value: Measurement value as float.
    """

    # Obtain reference to all logging files
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get("files", [])

    # Download files
    for item in items:
        print("Downloading {}...".format(item["name"]))
        download_file(item["id"], os.path.join(tempdir, item["name"]), service)

    # Load all data into a single data frame
    print("Processing data into single file...")
    dfs = []
    for fn in os.listdir(tempdir):
        df = load_function(os.path.join(tempdir, fn), fields)
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


def download_file(file_id, filename, service):
    """
    Downloads a file from Google Drive.

    Args:
        - file_id (str): File ID as used by Google Drive.
        - filename (str): File path to save file to.
        - service (Resource): A googleapiclient.discovery.Resource object.

    Returns:
        None, saves file to disk as a side-effect.
    """
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))

    fh.seek(0)
    with open(filename, "wb") as outfile:
        shutil.copyfileobj(fh, outfile)


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

    # Limit fields to those required
    clean_fields = [{field: row[field] for field in fields.keys()} for row in importer.records]

    # Convert fields to ISO8061/metric
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


def wide_to_long(data):
    """
    Converts wide dataframe into long.

    Args:
        - data (pandas.DataFrame): Wide dataframe with a 'timestamp'
        column and at least one other measurement column.

    Returns:
        A pandas.DataFrame object with 3 columns:
            - timestamp: In YYYY-mm-dd HH:MM:SS format
            - measurand: Name of measurand as human readable string
            - value: Measurement value as float.
    """
    # Convert to long
    long = data.melt(id_vars="timestamp", var_name="measurand", value_name="value")
    long = long.set_index("timestamp")

    return long


if __name__ == "__main__":
    main()
