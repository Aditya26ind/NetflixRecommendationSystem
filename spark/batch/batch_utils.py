from pyspark.sql import SparkSession


def create_spark(app_name: str = "batch-job") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
