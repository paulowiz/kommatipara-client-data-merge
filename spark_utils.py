# import modules
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf 
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import os
from datetime import datetime
from typing import List

class SparkUtils:
    """
    Class with all generic functions using spark.
    """

    def __init__(self, app_name:str) -> None:
        """
        Constructor class.

        :param app_name: spark app name
        :type app_name: str
        """
        self.spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master(os.environ.get("SPARK_MASTER_URL", "local[*]"))\
            .config("spark.driver.host","127.0.0.1") \
            .config("spark.driver.bindAddress","127.0.0.1")\
            .config("spark.local.dir","tmp")\
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def read_csv_to_spark_dataframe(self, path:str) -> DataFrame:
        """
        Read a csv into spark dataframe.

        :param path: file path
        :type path: str
        :return: spark dataframe
        :rtype: DataFrame
        """ 
        return self.spark.read.option("delimiter", ",").option("header", "true").csv(path)
    
    def rename_columns_from_dataframe(self, dataframe: DataFrame, old_columns: List[str], new_columns: List[str]) -> DataFrame:
        """
        Rename columns from spark dataframe.

        :param dataframe: spark dataframe
        :type dataframe: DataFrame
        :param old_columns: list of current column names.
        :type old_columns: List[str]
        :param new_columns: list of new column names.
        :type new_columns: List[str]
        :raises ValueError: String with error.
        :return: spark Dataframe with columns renamed.
        :rtype: DataFrame
        """
        # Check if the lengths of old_columns and new_columns lists are the same
        if len(old_columns) != len(new_columns):
            raise ValueError("The number of old column names and new column names must be the same.")

        #clean columns name, removing extra spaces.
        dataframe = dataframe.select([col(name).alias(name.strip()) for name in dataframe.columns])
        
        # Create a mapping of old column names to new column names
        column_mapping = dict(zip(old_columns, new_columns))

        # Rename the columns in the DataFrame
        for old_col, new_col in column_mapping.items():
            dataframe = dataframe.withColumnRenamed(old_col, new_col)

        return dataframe
    
    def drop_columns_from_dataframe(self, dataframe: DataFrame, columns_to_drop: List[str]) -> DataFrame:
        """
        Drop columns from a spark dataframe.

        :param dataframe: spark dataframe
        :type dataframe: DataFrame
        :param columns_to_drop: list of column names.
        :type columns_to_drop: List[str]
        :return: spark dataframe.
        :rtype: DataFrame
        """
        # Drop the specified columns from the DataFrame
        dataframe = dataframe.drop(*columns_to_drop)
        return dataframe
    
    def filter_dataframe_by_country(self, dataframe: DataFrame, country_column: str, countries: list) -> DataFrame:
        """
        Filter dataframe by country column and list of countries.

        :param dataframe: spark Dataframe.
        :type dataframe: DataFrame
        :param country_column: target column about countries.
        :type country_column: str
        :param countries: list of countries.
        :type countries: list
        :return: spark Dataframe.
        :rtype: DataFrame
        """
        # Use the filter method to filter the DataFrame based on the country_column
        filtered_dataframe = dataframe.filter(dataframe[country_column].isin(countries))
        return filtered_dataframe
    
    def join_two_dataframes(self, first_dataframe: DataFrame, second_dataframe: DataFrame, key_column: str, join_type: str) -> DataFrame:
        """
        Join two spark dataframe by a column key.

        :param first_dataframe: spark Dataframe
        :type first_dataframe: DataFrame
        :param second_dataframe: spark Dataframe
        :type second_dataframe: DataFrame
        :param key_column: key column.
        :type key_column: str
        :param join_type: join type (inner, left, right).
        :type join_type: str
        :return: spark Dataframe.
        :rtype: DataFrame
        """
        return first_dataframe.join(second_dataframe, key_column, join_type)

    def destroy_spark_connection(self):
        """
        Stop spark connection.
        """
        self.spark.stop()
