#!/bin/bash

# Source environment variables
source cdp.variables

# Perform Kerberos authentication
kinit kerberos_principle -k -t keytabfile

# Path to your configuration file
CONFIG_FILE="config.txt"

# Path to the Python script
PYTHON_SCRIPT="coalesce_parquet.py"  # Replace this with the correct path to your Python script

# Create a unique log file for this run (with timestamp)
LOG_FILE="coalesce_run_$(date +'%Y%m%d_%H%M%S').log"

# Spark-submit parameters (you can adjust these based on your needs)
QUEUE="default"           # Specify the queue to submit the job
NUM_EXECUTORS=4           # Number of executors
EXECUTOR_MEMORY="4G"      # Memory per executor
DRIVER_MEMORY="2G"        # Memory for the driver
NUM_CORES=2               # Number of cores per executor
SPARK_CONF_OPTIONS="--conf spark.some.config.option=value"  # Any additional Spark config options

# Function to log messages
log_message() {
    local message=$1
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $message" | tee -a "$LOG_FILE"
}

# Function to call the Python script using spark-submit for each Parquet file
process_parquet_files() {
    local hdfs_dir=$1

    # List all Parquet files in the HDFS directory
    parquet_files=$(hdfs dfs -ls "$hdfs_dir" | grep ".parquet" | awk '{print $8}')

    # Iterate over each Parquet file
    for parquet_file in $parquet_files; do
        log_message "Processing Parquet file: $parquet_file"

        # Set the environment variable for the Python script
        export HDFS_PARQUET_PATH="$parquet_file"

        # Call the Python script using spark-submit with necessary parameters
        spark-submit \
            --master yarn \
            --deploy-mode cluster \
            --queue "$QUEUE" \
            --num-executors "$NUM_EXECUTORS" \
            --executor-memory "$EXECUTOR_MEMORY" \
            --driver-memory "$DRIVER_MEMORY" \
            --executor-cores "$NUM_CORES" \
            $SPARK_CONF_OPTIONS \
            "$PYTHON_SCRIPT" >> "$LOG_FILE" 2>&1

        # Check if the command was successful
        if [ $? -eq 0 ]; then
            log_message "Successfully processed: $parquet_file"
        else
            log_message "Failed to process: $parquet_file"
        fi
    done
}

# Read each HDFS path from the config file and process the Parquet files in it
while IFS= read -r hdfs_path; do
    log_message "Processing HDFS directory: $hdfs_path"

    process_parquet_files "$hdfs_path"
    
done < "$CONFIG_FILE"

log_message "Coalescing run complete. Log file: $LOG_FILE"
