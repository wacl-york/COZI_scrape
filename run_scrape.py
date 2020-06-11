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

LOCAL_DIR = "tmp"
CREDENTIALS_FN = "credentials.json"
COLUMNS_FN = "fields.json"


def main():
    args = parse_args()

    # Create local directory to store data
    if os.path.exists(LOCAL_DIR):
        cleanup()
    os.makedirs(LOCAL_DIR)

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

    # Obtain reference to all logging files
    results = (
        service.files()
        .list(q="name contains 'logging'", fields="files(id, name)")
        .execute()
    )
    items = results.get("files", [])

    # Download files
    for item in items:
        print("Downloading {}...".format(item["name"]))
        download_file(item["id"], os.path.join(LOCAL_DIR, item["name"]), service)

    # Load all data into a single data frame
    print("Processing data into single file...")
    dfs = []
    for fn in os.listdir(LOCAL_DIR):
        df = load_file(os.path.join(LOCAL_DIR, fn), field_names)
        if df is None:
            continue

        dfs.append(df)

    # Combine all clean datasets into 1 frame and convert to long
    if len(dfs) < 1:
        print("Error: no clean data loaded, terminating execution")
        cleanup()
        return

    combined = pd.concat(dfs)
    long_df = wide_to_long(combined)

    # Save file
    try:
        long_df.to_csv(args.output)
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


def load_file(filename, fields):
    """
    Reads CSV file into memory.

    Subsets dataset into fields of interest and converts timestamp from Excel
    format into ISO 8061.

    Args:
        - filename (str): File to load.
        - fields (dict): Mapping between {raw_label: clean_label}, where
            raw_label is the column name in the raw data on Google Drive, and
            clean_label is the desired label for our output data.

    Returns:
        A pandas.DataFrame object with as many columns as there are entries in
        fields.json, with the columns set as the attributes of this JSON file if
        the read is succesful, None otherwise..
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
