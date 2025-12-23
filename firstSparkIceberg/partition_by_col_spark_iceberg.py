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
config = getattr(configuration, f"{ENV}_CONFIG", None)


if config is None:
    logger.error(f"No configuration found for environment: {ENV}")
    raise ValueError(f"No configuration found for environment: {ENV}")

conf = InitSparkConfig(config, "PartitionByColSparkIcebergApp").create_spark_conf()

spark = SparkSession.builder.config(conf=conf).master("local[*]").getOrCreate()


logger.info("Creating the table ...")

try:
    spark.sql("""
                CREATE TABLE IF NOT EXISTS glue.spark_iceberg_local.emp_partitioned_month (
                    id INT,
                    role STRING,
                    department STRING,
                    join_date DATE
                )               
                USING ICEBERG
                PARTITIONED BY (months(join_date))
    """)
except Exception as e:
    logger.error(f"Error creating table: {e}")
    raise
logger.info("Table created successfully!")
