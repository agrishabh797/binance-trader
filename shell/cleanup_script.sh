#!/bin/bash

# Define the path to the logs directory
logs_directory="/home/ubuntu/logs"

# Calculate the date n days ago in the YYYYMMDD format
days_ago=$(date -d "2 days ago" +%Y%m%d)

# Loop through the directories in the logs directory
for dir in "$logs_directory"/*; do
    # Check if the directory name is in YYYYMMDD format
    if [[ -d "$dir" && $(basename "$dir") =~ ^[0-9]{8}$ ]]; then
        dir_date=$(basename "$dir")
        echo $dir_date

        # Compare the directory date with days_ago
        if [[ "$dir_date" -lt "$days_ago" ]]; then
            echo "Deleting directory: $dir"
            rm -rf "$dir"
        fi
    fi
done