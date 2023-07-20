# test_spark_utils.py

import pytest
from pyspark.sql import SparkSession
from chispa import assert_df_equality,assert_column_equality
from spark_utils import SparkUtils
import os 

def test_read_csv_to_spark_dataframe():
    # Test data
    test_csv_path = "test_data.csv"
    test_data = "ID,Name,Score\n1,John,85\n2,Jane,90\n3,Bob,78\n"
    with open(test_csv_path, "w") as file:
        file.write(test_data)

    spark_utils = SparkUtils()

    # Create SparkSession for the test
    test_spark_session = spark_utils.create_connection_spark("test_app")

    # Read CSV into Spark DataFrame
    df = spark_utils.read_csv_to_spark_dataframe(test_csv_path, test_spark_session)

    # Define the expected DataFrame
    expected_data = [(1, "John", "85"), (2, "Jane", "90"), (3, "Bob", "78")]
    expected_columns = ["ID", "Name", "Score"]
    expected_df = test_spark_session.createDataFrame(expected_data, expected_columns)

    # Use chispa's assert_df_equality to compare DataFrames
    assert_df_equality(df, expected_df, ignore_row_order=True)

    # Clean up the temporary CSV file
    test_spark_session.stop()
    test_spark_session._jvm.System.gc()
    assert not os.path.exists(test_csv_path)

def test_rename_columns_from_dataframe():

    spark_utils = SparkUtils()
    test_spark_session = spark_utils.create_connection_spark("test_app")
    # Test data
    data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 22)]
    df = test_spark_session.createDataFrame(data, ["ID", "Name", "Age"])

    new_columns = ["PersonID", "FullName", "Years"]

    renamed_df = spark_utils.rename_columns_from_dataframe(df, ["ID", "Name", "Age"], new_columns)

    # Define the expected DataFrame
    expected_data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 22)]
    expected_columns = ["PersonID", "FullName", "Years"]
    expected_df = test_spark_session.createDataFrame(expected_data, expected_columns)

    # Use chispa's assert_df_equality to compare DataFrames
    assert_column_equality(renamed_df, expected_df)

    test_spark_session.stop()
    test_spark_session._jvm.System.gc()

def test_drop_columns_from_dataframe():

    spark_utils = SparkUtils()
    test_spark_session = spark_utils.create_connection_spark("test_app")

    # Test data
    data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 22)]
    df = test_spark_session.createDataFrame(data, ["ID", "Name", "Age"])

    df_without_age = spark_utils.drop_columns_from_dataframe(df, ["Age"])

    # Define the expected DataFrame
    expected_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    expected_columns = ["ID", "Name"]
    expected_df = test_spark_session.createDataFrame(expected_data, expected_columns)

    # Use chispa's assert_df_equality to compare DataFrames
    assert_df_equality(df_without_age, expected_df,ignore_row_order=True)

    test_spark_session.stop()
    test_spark_session._jvm.System.gc()

