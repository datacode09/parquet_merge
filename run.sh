#!/bin/bash

# Path to your configuration file
CONFIG_FILE="config.txt"

# Path to the Python script
PYTHON_SCRIPT="coalesce_parquet.py"  # Replace this with the correct path to your Python script

# Function to call the Python script for each Parquet file
process_parquet_files() {
    local hdfs_dir=$1

    # List all Parquet files in the HDFS directory
    parquet_files=$(hdfs dfs -ls "$hdfs_dir" | grep ".parquet" | awk '{print $8}')

    # Iterate over each Parquet file
    for parquet_file in $parquet_files; do
        echo "Processing Parquet file: $parquet_file"

        # Set the environment variable for the Python script
        export HDFS_PARQUET_PATH="$parquet_file"

        # Call the Python script to process the Parquet file
        python3 "$PYTHON_SCRIPT"
        
        if [ $? -eq 0 ]; then
            echo "Successfully processed: $parquet_file"
        else
            echo "Failed to process: $parquet_file"
        fi
    done
}

# Read each HDFS path from the config file and process the Parquet files in it
while IFS= read -r hdfs_path; do
    echo "Processing HDFS directory: $hdfs_path"

    process_parquet_files "$hdfs_path"
    
done < "$CONFIG_FILE"

