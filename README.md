# COZI downloader

Downloads COZI instrument data from Google Drive and prepares it for analysis.

The program firstly downloads the raw logging files from Google Drive into a temporary local folder.
The logging files identified as those having a filename starting with _logging_ and being stored in a directory that the Google Service account has access to.
Then, the program collates all of the data into a single data table, subsets it to just those columns of interest, and saves the resultant data as a long CSV file to a specified filename.
The output CSV has 3 columns:

  - `timestamp`: In YYYY-mm-dd HH:MM:SS format
  - `measurand`: Name of measurand as human readable string
  - `value`: Measurement value as float.

The file `fields.json` specifies the column names of the measurements to be included in the output CSV, along with their label.

# Dependencies

The script requires Python 3 (it has been tested on 3.6 and 3.7) along with several dependencies:

  - `googleapliclient`
  - `google.oauth2`
  - `pandas`

These can be installed into your Python environment by using `pip install -r requirements.txt`.

The program also requires a Google Service account to be setup with read access to the Google Drive folder where the data resides.
The credential for this account must be saved as `credentials.json` and will be required for this program to run successfully.

# Installation

After setting up the Python dependencies, simply clone this repository, then create the `credentials.json` file containing the Google Service account credentials.

The required aspects for deployment are:
  - The `run_scrape.py` script that handles the downloading and processing of the data
  - The `credentials.json` file
  - The `fields.json` file that describes the mapping between column labels in the raw and output processed data

# Running

The program is run through the command

`python run_scrape.py <output filename>`

Where `<output filename>` is the location that the processed CSV will be saved to.

