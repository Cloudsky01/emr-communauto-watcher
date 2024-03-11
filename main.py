from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("PythonSparkExample") \
        .getOrCreate()

    # Read data from S3 or HDFS, change the path accordingly
    input_data = spark.read.option("header", "true").csv("s3://your-bucket/input.csv")

    # Perform some transformations
    result = input_data.groupBy("column1").count()

    # Write the result back to S3 or HDFS
    result.write.mode("overwrite").csv("s3://your-bucket/output/")

    # Stop the SparkSession
    spark.stop()
