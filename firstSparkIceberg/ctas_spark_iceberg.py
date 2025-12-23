from firstSparkIceberg.config import configuration
from firstSparkIceberg.config.get_saprk_conf import InitSparkConfig
from dotenv import load_dotenv
import logging
import os
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
ENV = os.getenv("RUNTIME")

if ENV is None:
    logger.error("RUNTIME environment variable is not set.")
    raise ValueError("RUNTIME environment variable is not set.")

config = getattr(configuration, f"{ENV}_CONFIG", None)

if config is None:
    logger.error(f"No configuration found for environment: {ENV}")
    raise ValueError(f"No configuration found for environment: {ENV}")

conf = InitSparkConfig(config, "CTASSparkIcebergApp").create_spark_conf()

spark = SparkSession.builder.config(conf=conf).master("local[*]").getOrCreate()

spark.sql("""
            CREATE TABLE IF NOT EXISTS glue.spark_iceberg_local.ctas_emp 
            USING ICEBERG
            AS
            SELECT * FROM glue.spark_iceberg_local.emp_partitioned_month
""")

logger.info("USING DATAFRMAE API TO CREATE TABLE ...")

df = spark.read.table("glue.spark_iceberg_local.emp_partitioned_month")

try:
    df.writeTo("glue.spark_iceberg_local.ctas_emp_from_dataframe").create()
except Exception as e:
    raise RuntimeError(f"Error creating table using DataFrame API: {e}")

logger.info("Table created successfully using DataFrame API!")

"""

In this CTAS(Create Table As Select) example the table properties of the parent table are
not inherited by the child table. For example if the parent table is partitioned on a column,
the child table will not be partitioned on that column unless explicitly specified using the PARTITIONED BY clause
and the TBLPROPERTIES command.
"""