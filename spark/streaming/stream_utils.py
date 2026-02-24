from pyspark.sql import SparkSession


def create_spark(app_name: str = "stream-events") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
