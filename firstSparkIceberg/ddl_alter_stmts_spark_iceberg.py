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

conf = InitSparkConfig(config, "DdlAlterStmtsSparkIceberg").create_spark_conf()

spark = SparkSession.builder.config(conf=conf).master("local[*]").getOrCreate()

logger.info("Adding a columns")
add_column = """

    ALTER TABLE glue.spark_iceberg_local.customers ADD COLUMN manager STRING, manager_id INT;

"""
logger.info(add_column)
spark.sql(add_column)

logger.info("Dropping a column...")
drop_column = """

ALTER TABLE glue.spark_iceberg_local.customers DROP COLUMN manager_id, manager;

"""
logger.info(drop_column)
spark.sql(drop_column)

