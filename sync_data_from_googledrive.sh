#!/bin/sh

cd ~/WACL/COZI_scrape

# Downloads latest raw data
rclone --config rclone.conf --include *.wlk -v --drive-shared-with-me sync CoziDrive:WACLroof raw_data/MET
rclone --config rclone.conf --include logging* -v --drive-shared-with-me sync CoziDrive:COZI_DATA raw_data/AQ

# Pre-processes it into a single CSV
~/.conda/envs/coziscrape/bin/python run_scrape.py clean_data/cozi_data.csv

# Uploads for Shiny
cp clean_data/cozi_data.csv /shared/storage/shiny0/cozi/data.csv
# Uploads for Grafana
rclone --config rclone.conf --include cozi_data.csv -v --drive-shared-with-me sync clean_data CoziDrive:COZIDataForGrafana
