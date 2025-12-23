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

conf = InitSparkConfig(config, "CTASSparkIcebergApp").create_spark_conf()
spark = SparkSession.builder.config(conf=conf).master("local[*]").getOrCreate()

spark.sql("""
             CREATE TABLE glue.spark_iceberg_local.emp_ctas_partition_tbl_props
             USING iceberg    
             PARTITIONED BY (join_date)    
             TBLPROPERTIES (write.format.default='avro')    
             AS SELECT *    
             FROM glue.spark_iceberg_local.emp_partitioned_month
""")