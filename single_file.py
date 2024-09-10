from pyspark.sql import SparkSession
import logging
import os
from py4j.java_gateway import java_import

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()

def get_filesystem_manager(spark_context):
    # Import Hadoop FileSystem and Path classes
    java_import(spark_context._gateway.jvm, 'org.apache.hadoop.fs.Path')
    FileSystem = spark_context._jvm.org.apache.hadoop.fs.FileSystem
    return FileSystem.get(spark_context._jsc.hadoopConfiguration())

def read_parquet_file(spark, file_path):
    logging.info(f"Reading Parquet file: {file_path}")
    return spark.read.parquet(file_path)

def write_parquet_file(df, output_dir):
    logging.info(f"Writing coalesced Parquet file to {output_dir}")
    df.coalesce(1).write.mode("overwrite").parquet(output_dir)

def delete_old_file(fs, spark_context, file_path):
    logging.info(f"Deleting old Parquet file: {file_path}")
    fs.delete(spark_context._gateway.jvm.Path(file_path), True)

def rename_file(fs, spark_context, src, dst):
    logging.info(f"Renaming {src} to {dst}")
    fs.rename(spark_context._gateway.jvm.Path(src), spark_context._gateway.jvm.Path(dst))

def get_row_count(df):
    count = df.count()
    logging.info(f"Row count: {count}")
    return count

def process_parquet_file(spark, fs, parquet_file_path):
    spark_context = spark.sparkContext

    try:
        logging.info(f"Processing Parquet file: {parquet_file_path}")

        # Pre-count: Read the Parquet file into a Spark DataFrame and get the row count
        df = read_parquet_file(spark, parquet_file_path)
        pre_count = get_row_count(df)

        # Step 1: Write the DataFrame into a single coalesced Parquet file in a temp directory
        parent_dir = os.path.dirname(parquet_file_path)
        temp_output_dir = parent_dir + "/coalesced_temp"
        write_parquet_file(df, temp_output_dir)

        # Step 2: Get the path of the coalesced Parquet file
        temp_parquet_file = [f.getPath().toString() for f in fs.listStatus(spark_context._gateway.jvm.Path(temp_output_dir)) if f.getPath().getName().endswith(".parquet")][0]
        final_parquet_file = parquet_file_path  # We're replacing the original file

        # Step 3: Delete the old Parquet file
        delete_old_file(fs, spark_context, parquet_file_path)

        # Step 4: Rename the coalesced Parquet file to the original file name
        rename_file(fs, spark_context, temp_parquet_file, final_parquet_file)

        # Step 5: Clean up the temporary directory
        fs.delete(spark_context._gateway.jvm.Path(temp_output_dir), True)

        # Post-count: Read the coalesced Parquet file and get the row count
        df_coalesced = read_parquet_file(spark, final_parquet_file)
        post_count = get_row_count(df_coalesced)

        # Step 6: Compare row counts
        if pre_count == post_count:
            logging.info(f"Row counts match: Pre-count = {pre_count}, Post-count = {post_count}")
        else:
            logging.error(f"Row counts do not match: Pre-count = {pre_count}, Post-count = {post_count}")
            # Handle error: You can implement rollback or additional error handling here

    except Exception as e:
        logging.error(f"Error processing Parquet file {parquet_file_path}: {e}")

def main():
    # Step 1: Fetch the input HDFS Parquet file path from environment variable
    parquet_file_path = os.getenv("HDFS_PARQUET_PATH")

    if not parquet_file_path:
        logging.error("HDFS_PARQUET_PATH environment variable is not set.")
        return

    # Step 2: Create Spark session
    spark = create_spark_session("SingleParquetFileCoalesce")

    # Step 3: Get Hadoop FileSystem manager
    fs = get_filesystem_manager(spark.sparkContext)

    # Step 4: Process the Parquet file
    process_parquet_file(spark, fs, parquet_file_path)

    # Step 5: Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
