import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Pytest fixture to create a SparkSession for the entire test session.
    This makes the 'spark' object available to all test functions.
    """
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("TestRunner")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()