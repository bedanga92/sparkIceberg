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

spark.sql(""" SELECT * FROM glue.spark_iceberg_local.employees_new""").show()

logger.info("Selecting all the sales employees from the Iceberg table")

spark.sql(""" SELECT * FROM glue.spark_iceberg_local.employees_new WHERE department='Finance' """).show()