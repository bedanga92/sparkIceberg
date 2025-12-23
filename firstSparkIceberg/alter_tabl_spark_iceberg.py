from firstSparkIceberg.config import configuration
from firstSparkIceberg.config.get_saprk_conf import InitSparkConfig
from dotenv import load_dotenv
import os
import logging
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

conf = InitSparkConfig(config, "SparkScalableIcebergApp").create_spark_conf()
spark = SparkSession.builder.config(conf=conf).master("local[*]").getOrCreate()

create_table_sql = """
CREATE TABLE IF NOT EXISTS glue.spark_iceberg_local.customers_temp (
    customer_id INT,
    name STRING,
    email STRING,
    age INT,
    account_balance DOUBLE,
    registration_date DATE
)
USING iceberg
PARTITIONED BY (days(registration_date))
"""

spark.sql(create_table_sql)

logger.info("Altering table spark_iceberg_local.customers_temp to rename it to customers_new...")
alter_table_sql = """

ALTER TABLE glue.spark_iceberg_local.customers_temp RENAME TO customers_new
"""

logger.info(f"Executing SQL: {alter_table_sql}")
spark.sql(alter_table_sql)
logger.info("Table renamed successfully!")
