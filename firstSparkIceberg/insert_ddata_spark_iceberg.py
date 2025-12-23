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

logger.info("Inserting data into Iceberg table with order and distribution settings")

spark.sql(""" CREATE TABLE IF NOT EXISTS glue.spark_iceberg_local.employees_new (
            id INT,
            designation STRING,
            department STRING,
            salary INT,
            region STRING
        ) USING ICEBERG
      
""")

spark.sql("""
INSERT INTO glue.spark_iceberg_local.employees_new 
        VALUES 
        (1, 'Software Engineer', 'Engineering', 25000, 'NA'),
        (2, 'Director', 'Sales', 22000, 'EMEA'),
        (3, 'Manager', 'HR', 20000, 'APAC'),
        (4, 'Analyst', 'Finance', 18000, 'NA'),
        (5, 'Consultant', 'Consulting', 21000, 'EMEA'),
        (6, 'Intern', 'Engineering', 15000, 'APAC'),
        (7, 'VP', 'Marketing', 30000, 'NA'),
        (8, 'CFO', 'Finance', 35000, 'EMEA'),
        (9, 'CEO', 'Executive', 50000, 'APAC'),
        (10, 'Support Engineer', 'Support', 16000, 'NA'),
        (11, 'Product Manager', 'Product', 24000, 'EMEA'),
        (12, 'Data Scientist', 'Data', 27000, 'APAC')
""")

