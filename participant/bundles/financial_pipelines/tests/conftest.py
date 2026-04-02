import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession for unit tests.

    On Databricks serverless, reuse the active SparkSession from the cluster.
    Locally, build a new one for testing.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("financial-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
