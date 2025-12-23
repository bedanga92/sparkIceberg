from firstSparkIceberg.config import configuration
from firstSparkIceberg.config.get_saprk_conf import InitSparkConfig
from dotenv import load_dotenv
import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

ENV = os.getenv("RUNTIME")

config = getattr(configuration, f"{ENV}_CONFIG", None)

if config is None:
    logger.error(f"No configuration found for environment: {ENV}")
    raise ValueError(f"No configuration found for environment: {ENV}")

conf = InitSparkConfig(config, "DataframeSparkIcebergApp").create_spark_conf()

spark = SparkSession.builder.config(conf=conf).master("local[*]").getOrCreate()

schema = StructType([StructField("id", IntegerType(), True),
                     StructField("role", StringType(), True),
                     StructField("department", StringType(), True)])

empty_rdd = spark.sparkContext.emptyRDD()
df = spark.createDataFrame(empty_rdd, schema=schema)
df.writeTo("glue.spark_iceberg_local.employees").using("iceberg").create()
logger.info("Table employees created successfully using DataFrame API!")
