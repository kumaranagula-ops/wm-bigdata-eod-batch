import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # One local session for the whole test run: starting Spark per test is slow.
    # No Hive support here; the transforms under test never touch the catalog.
    s = (SparkSession.builder.master("local[2]").appName("wm-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.catalogImplementation", "in-memory")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()
