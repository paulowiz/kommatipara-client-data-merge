# import modules
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf 
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import os
from datetime import datetime
from typing import List


class SparkUtils:
    """Class with all generic functions using spark."""

    def __init__(self,app_name:str) -> None:
        """Contructor class

        Args:
            app_name (str): spark app name
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
        pass

    def read_csv_to_spark_dataframe(self, path:str) -> DataFrame:
        """Read a csv into spark dataframe.

        Args:
            path (str): file path

        Returns:
            DataFrame: spark dataframe
        """ 
        return self.spark.read.option("delimiter", ",").option("header", "true").csv(path)
    
    def rename_columns_from_dataframe(self,dataframe: DataFrame, old_columns: List[str], new_columns: List[str]) -> DataFrame:
        """Rename columns from spark dataframe.

        Args:
            dataframe (DataFrame): spark dataframe
            old_columns (List[str]): list of current column names.
            new_columns (List[str]): list of new column mames.

        Raises:
            ValueError: String with error.

        Returns:
            DataFrame: spark Dataframe with columns renamed.
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
    
    def drop_columns_from_dataframe(self,dataframe: DataFrame, columns_to_drop: List[str]) -> DataFrame:
        """drop columns from a spark dataframe.

        Args:
            dataframe (DataFrame): spark dataframe
            columns_to_drop (List[str]): list of column names.

        Returns:
            DataFrame: spark dataframe.
        """
        # Drop the specified columns from the DataFrame
        dataframe = dataframe.drop(*columns_to_drop)
        return dataframe
    
    def filter_dataframe_by_country(self,dataframe: DataFrame, country_column: str, countries: list) -> DataFrame:
        """filter dataframe by country column and list of countries.

        Args:
            dataframe (DataFrame): spark Dataframe.
            country_column (str): target column about countries.
            countries (list): list of countries.

        Returns:
            DataFrame: spark Dataframe.
        """
        # Use the filter method to filter the DataFrame based on the country_column
        filtered_dataframe = dataframe.filter(dataframe[country_column].isin(countries))
        return filtered_dataframe
    
    def join_two_dataframes(self, first_dataframe:DataFrame,second_dataframe:DataFrame,key_column:str,join_type:str) -> DataFrame:
        """Join two spark dataframe by a column key.

        Args:
            first_dataframe (DataFrame): spark Dataframe
            second_dataframe (DataFrame): spark Dataframe
            key_column (str): key column.
            join_type (str): join type(inner,left,right).

        Returns:
            DataFrame: spark Dataframe.
        """
        return first_dataframe.join(second_dataframe, key_column, join_type)

   
    def destroy_spark_connection(self):
        """Stop spark connection.
        """
        self.spark.stop()