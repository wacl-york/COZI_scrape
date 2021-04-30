#!/bin/sh

# Runs the COZI scraper
cd ~/WACL/COZI_scrape
rclone --config rclone.conf --include *.wlk -v --drive-shared-with-me sync CoziDrive:WACLroof raw_data/MET
rclone --config rclone.conf --include logging* -v --drive-shared-with-me sync CoziDrive:COZI_DATA raw_data/AQ

~/.conda/envs/coziscrape/bin/python run_scrape.py /shared/storage/shiny0/cozi/data.csv
