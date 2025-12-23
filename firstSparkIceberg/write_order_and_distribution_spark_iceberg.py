from firstSparkIceberg.config import configuration
from firstSparkIceberg.config.get_saprk_conf import InitSparkConfig
from dotenv import load_dotenv
import logging
import os
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

ENV = os.getenv("RUNTIME")

config = getattr(configuration, f"{ENV}_CONFIG", None)

if config is None:
    logger.error(f"No Configuration found for this environment")
    raise ValueError(f"No Configuration found for this environment: {ENV}")

conf = InitSparkConfig(config, "SparkAppOrderAndDistribution").create_spark_conf()

spark = SparkSession.builder.config(conf=conf).master("local[*]").getOrCreate()

spark.sql(""" 
            ALTER TABLE glue.spark_iceberg_local.employees WRITE ORDERED BY id ASC 
            """)

spark.sql("""
            ALTER TABLE glue.spark_iceberg_local.employees WRITE DISTRIBUTED BY PARTITION 
            """)
