import json

from pyspark.sql import SparkSession


class SparkConfig:
    """Configuration for Spark sessions."""

    def __init__(self, app_name: str):
        self.app_name = app_name
        self.spark = self.create_spark_session()

    def create_spark_session(self) -> SparkSession:
        """
        Creates and returns a SparkSession configured using parameters from
        JSON configuration file.

        The method reads Spark configuration options from
        '/opt/airflow/config/spark_config.json', applies them to the
        SparkSession builder, and initializes the session.

        Returns:
            SparkSession: An initialized SparkSession object with the specified
            configurations.
        """
        with open("/opt/airflow/config/spark_config.json") as f:
            spark_config = json.load(f)

        spark = SparkSession.builder.appName(self.app_name)

        for key, value in spark_config.items():
            spark = spark.config(key, value)

        spark = spark.getOrCreate()

        return spark
