#!/bin/bash

# Path to your config file
CONFIG_FILE="config.conf"

# Create a unique log file for this run (with timestamp)
LOG_FILE="coalesce_run_$(date +'%Y%m%d_%H%M%S').log"

# -----------------------------------------
# Function to log messages
# -----------------------------------------
log_message() {
    local message=$1
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $message" | tee -a "$LOG_FILE"
}

# -----------------------------------------
# Function to read a parameter from the config file
# -----------------------------------------
get_config_value() {
    local key=$1
    grep -E "^$key[[:space:]]*=" "$CONFIG_FILE" | sed -E "s/$key[[:space:]]*=[[:space:]]*//"
}

# -----------------------------------------
# Function to parse the pseudo-list format from the config file
# -----------------------------------------
get_hdfs_paths() {
    # Extract the list of HDFS paths from the config file and clean up the format
    grep -A 10 "hdfs_paths" "$CONFIG_FILE" | sed -e 's/hdfs_paths = \[//' -e 's/]//' -e 's/,//g' -e 's/^ *//g' -e 's/ *$//g' | tr '\n' ' '
}

# -----------------------------------------
# Function to perform Kerberos authentication
# -----------------------------------------
perform_kerberos_authentication() {
    local kerberos_principal=$1
    local keytab_file=$2
    kinit "$kerberos_principal" -k -t "$keytab_file"
    if [ $? -eq 0 ]; then
        log_message "Kerberos authentication succeeded for $kerberos_principal"
    else
        log_message "Kerberos authentication failed for $kerberos_principal"
        exit 1
    fi
}

# -----------------------------------------
# Function to list all Parquet files in the HDFS directory
# -----------------------------------------
list_parquet_files() {
    local hdfs_dir=$1
    parquet_files=$(hdfs dfs -ls "$hdfs_dir" | grep ".parquet" | awk '{print $8}')
    echo "$parquet_files"
}

# -----------------------------------------
# Function to call the Python script using spark-submit for each Parquet file
# -----------------------------------------
submit_spark_job() {
    local parquet_file=$1
    local queue=$2
    local num_executors=$3
    local executor_memory=$4
    local driver_memory=$5
    local executor_cores=$6
    local spark_conf_options=$7
    local python_script=$8

    # Set the environment variable for the Python script
    export HDFS_PARQUET_PATH="$parquet_file"

    # Call the Python script using spark-submit with necessary parameters
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --queue "$queue" \
        --num-executors "$num_executors" \
        --executor-memory "$executor_memory" \
        --driver-memory "$driver_memory" \
        --executor-cores "$executor_cores" \
        $spark_conf_options \
        "$python_script" >> "$LOG_FILE" 2>&1

    # Check if the command was successful
    if [ $? -eq 0 ]; then
        log_message "Successfully processed: $parquet_file"
    else
        log_message "Failed to process: $parquet_file"
    fi
}

# -----------------------------------------
# Function to process all Parquet files in the HDFS directory
# -----------------------------------------
process_parquet_files_in_hdfs_directory() {
    local hdfs_dir=$1
    local queue=$2
    local num_executors=$3
    local executor_memory=$4
    local driver_memory=$5
    local executor_cores=$6
    local spark_conf_options=$7
    local python_script=$8

    log_message "Processing HDFS directory: $hdfs_dir"
    parquet_files=$(list_parquet_files "$hdfs_dir")

    if [ -z "$parquet_files" ]; then
        log_message "No Parquet files found in $hdfs_dir"
    else
        for parquet_file in $parquet_files; do
            log_message "Processing Parquet file: $parquet_file"
            submit_spark_job "$parquet_file" "$queue" "$num_executors" "$executor_memory" "$driver_memory" "$executor_cores" "$spark_conf_options" "$python_script"
        done
    fi
}

# -----------------------------------------
# Main function to orchestrate the entire process
# -----------------------------------------
main() {
    # Read values from the config file
    kerberos_principal=$(get_config_value "kerberos_principal")
    keytab_file=$(get_config_value "keytab_file")
    queue=$(get_config_value "queue")
    num_executors=$(get_config_value "num_executors")
    executor_memory=$(get_config_value "executor_memory")
    driver_memory=$(get_config_value "driver_memory")
    executor_cores=$(get_config_value "executor_cores")
    spark_conf_options=$(get_config_value "spark_conf_options")
    python_script="coalesce_parquet.py"  # Replace with the correct Python script path

    # Perform Kerberos authentication
    perform_kerberos_authentication "$kerberos_principal" "$keytab_file"

    # Get the HDFS paths from the config file
    hdfs_dirs=$(get_hdfs_paths)

    # Process each HDFS path
    for hdfs_dir in $hdfs_dirs; do
        process_parquet_files_in_hdfs_directory "$hdfs_dir" "$queue" "$num_executors" "$executor_memory" "$driver_memory" "$executor_cores" "$spark_conf_options" "$python_script"
    done

    log_message "Coalescing run complete. Log file: $LOG_FILE"
}

# Call the main function
main
