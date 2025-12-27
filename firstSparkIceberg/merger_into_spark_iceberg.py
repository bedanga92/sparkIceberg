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

logger.info("Creating the table now")
spark.sql(""" CREATE TABLE IF NOT EXISTS glue.spark_iceberg_local.employees_merge_new (
            id INT,
            designation STRING,
            department STRING,
            salary INT,
            region STRING
        ) USING ICEBERG

""")

logger.info("Inserting data into Iceberg table")
spark.sql("""INSERT INTO glue.spark_iceberg_local.employees_merge_new VALUES 
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
                (12, 'Data Scientist', 'Data', 27000, 'APAC'),
                (13, 'Manager', 'Operations', 120000, 'NA'),
                (14, 'Manager', 'IT', 95000, 'EMEA'),
                (15, 'Manager', 'Finance', 110000, 'APAC'),
                (16, 'Engineer', 'Engineering', 80000, 'NA'),
                (17, 'Director', 'Marketing', 130000, 'EMEA'),
                (18, 'Analyst', 'Data', 70000, 'APAC'),
                (19, 'Manager', 'Consulting', 105000, 'NA'),
                (20, 'Intern', 'HR', 40000, 'EMEA')
""")

spark.sql(""" CREATE TABLE IF NOT EXISTS glue.spark_iceberg_local.employees_merge_updates (
            id INT,
            designation STRING,
            department STRING,
            salary INT,
            region STRING
        ) USING ICEBERG

""")

spark.sql("""INSERT INTO glue.spark_iceberg_local.employees_merge_updates VALUES
                (3, 'Manager', 'HR', 21000, 'APAC'),
                (13, 'Manager', 'Operations', 125000, 'NA'),
                (14, 'Manager', 'IT', 98000, 'EMEA'),
                (15, 'Manager', 'Finance', 115000, 'APAC'),
                (19, 'Manager', 'Consulting', 108000, 'NA'),
                (21, 'Manager', 'Sales', 102000, 'EMEA'),
                (22, 'Manager', 'Support', 99000, 'APAC')
""")

logger.info("Merging data into Iceberg table")
spark.sql(""" MERGE INTO glue.spark_iceberg_local.employees_merge_new AS target 
              USING (SELECT * FROM glue.spark_iceberg_local.employees_merge_updates) AS source 
              ON target.id = source.id 
              WHEN MATCHED AND source.designation = 'Manager' AND source.salary > 100000 THEN
                  UPDATE SET target.salary = source.salary 
              WHEN NOT MATCHED THEN    
                    INSERT * """)
