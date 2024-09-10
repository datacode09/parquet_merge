import org.apache.spark.sql.SparkSession
import sys.process._

// Step 1: Initialize Spark Session
val spark = SparkSession.builder
    .appName("MergePartitionedParquetFiles")
    .getOrCreate()

// Path to the directory containing partitioned Parquet files
val inputPath = "hdfs://your_hdfs_path/prod/01559/app/RIEO/data_tde/ATOMDataFiles/Productivity/Turnpike/Daily_Outputs/turnpike_afa_output_daily_20240721.parquet"

// Temporary path for writing the merged file
val tempOutputPath = inputPath + "_temp"

// Step 2: Read the Parquet files from the directory
val df = spark.read.parquet(inputPath)

// Step 3: Coalesce to one partition (this will merge into a single file)
val dfCoalesced = df.coalesce(1)

// Step 4: Write the merged DataFrame to the temporary directory
dfCoalesced.write.mode("overwrite").parquet(tempOutputPath)

// Step 5: Rename the temporary output directory to the original directory
val deleteCommand = s"hdfs dfs -rm -r $inputPath"
val renameCommand = s"hdfs dfs -mv $tempOutputPath $inputPath"

// Delete the old directory
deleteCommand.!

// Rename the temporary output directory to the original
renameCommand.!

// Step 6: Stop the Spark session
spark.stop()
