from firstSparkIceberg.config import configuration
from firstSparkIceberg.config.get_saprk_conf import InitSparkConfig
import os
from dotenv import load_dotenv
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

ENV = os.getenv("RUNTIME")

config = getattr(configuration, f"{ENV}_CONFIG", None)

if config is None:
    raise ValueError(f"No configuration found for environment: {ENV}")

conf = InitSparkConfig(config, "AggSelectSparkIceberg").create_spark_conf()

spark = SparkSession.builder.config(conf=conf).getOrCreate()

logger.info("Executing the query")
spark.sql(""" SELECT AVG(salary) FROM glue.spark_iceberg_local.employees_new """).show()

logger.info("Operation Completed Successfully")
