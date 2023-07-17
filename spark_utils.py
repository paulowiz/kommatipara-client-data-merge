# import modules
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf 
from pyspark.sql.functions import col
import os
from datetime import datetime


class SparkUtils:

    def __init__(self,app_name:str) -> None:

        self.spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master(os.environ.get("SPARK_MASTER_URL", "local[*]"))\
            .config("spark.driver.host","127.0.0.1") \
            .config("spark.driver.bindAddress","127.0.0.1")\
            .config("spark.local.dir","tmp")\
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        pass

    def read_csv_to_spark_dataframe(self, path:str):
         """Read a csv into spark dataframe."""
         return self.spark.read.option("delimiter", ",").option("header", "true").csv(path)
   
    def destroy(self):
        
        self.spark.stop()