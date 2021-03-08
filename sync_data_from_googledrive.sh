#!/bin/sh

# Runs the COZI scraper
cd ~/WACL/COZI_scrape
rclone --config rclone.conf --include *.wlk -v --drive-shared-with-me sync CoziDrive:WACLroof raw_data
rclone --config rclone.conf --include logging* -v --drive-shared-with-me sync CoziDrive:COZI_DATA raw_data
