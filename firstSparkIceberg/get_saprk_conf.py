from typing import Dict
from pyspark import SparkConf


class InitSparkConfig:

    def __init__(self, json_config: Dict, app_name: str = "DefaultSparkIcebergApp"):
        self.json_config = json_config
        self.app_name = app_name

    def __get_spark_conf(self) -> str:
        return self.app_name

    def create_spark_conf(self) -> SparkConf:
        conf = SparkConf().setAppName(self.__get_spark_conf())

        for key, value in self.json_config.items():
            conf = conf.set(key, value)
        return conf
