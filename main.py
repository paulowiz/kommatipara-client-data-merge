# import modules
from spark_utils import SparkUtils
import sys,logging
from datetime import datetime
import os

folder_output = "client_data"
# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# current time variable to be used for logging purpose
dt_string = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
# change it to your app name
AppName = "MyPySparkApp"


def main(args):
    # start spark code
    sparkutils = SparkUtils()
    spark_session = sparkutils.create_connection_spark("MyApp")
    logger.info("Starting spark application")
    countries_list = args.countries.split(',')

 
    # Reading Client dataset
    logger.info("Client Dataset - Reading")
    df_client = sparkutils.read_csv_to_spark_dataframe(args.path1,spark_session)
    # Remove personal identifiable information columns
    logger.info("Client Dataset - Removing personal identifiable information columns")
    df_client = sparkutils.drop_columns_from_dataframe(df_client,["first_name","last_name"])
    # Rename id to client_identifier
    logger.info("Client Dataset - Renaming columns")
    df_client = sparkutils.rename_columns_from_dataframe(df_client,['id'],['client_identifier'])
    # filter countries
    logger.info("Client Dataset - Filtering dataframe by countries")
    df_client = sparkutils.filter_dataframe_by_country(df_client,"country", countries_list)
    logger.info("Client Dataset - Previewing Data")
    df_client.show()

    # Reading Financial dataset
    logger.info("Financial Dataset - Reading")
    df_Financial= sparkutils.read_csv_to_spark_dataframe(args.path2,spark_session)
    # Reanme columns 
    logger.info("Client Dataset - Renaming columns")
    df_Financial = sparkutils.rename_columns_from_dataframe(df_Financial,['id','btc_a','cc_t'],['client_identifier','bitcoin_address','credit_card_type'])
    #Drop credit card column
    df_Financial = sparkutils.drop_columns_from_dataframe(df_Financial,["cc_n"])
    logger.info("Financial Dataset - Previewing")
    df_Financial.show(truncate=False)
    
    #Joing datasets
    logger.info("Joing dataframes")
    df_joined = sparkutils.join_two_dataframes(df_client,df_Financial,'client_identifier','inner')
    logger.info("Preview joined dataset")
    df_joined.show(truncate=False)

    logger.info("Exporting result dataset using spark.write")
    dt_string = datetime.now().strftime("%Y%m%d_%H_%M_%S")
    
    if not os.path.exists('client_data'):
       os.makedirs('client_data')

    try:
        df_joined.write.csv(folder_output+'/'+dt_string+'_output.csv')
    except:
         logger.error("An error occurred in the previous step while attempting to write the data using `write.csv`")
         logger.info("Exporting dataset converting to pandas.")
         df_joined = df_joined.toPandas()
         df_joined.to_csv(folder_output+'/'+dt_string+'__output.csv')


    return None

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Process CSV files and list of countries.")
    parser.add_argument("--path1", type=str, help="Path to the client CSV file")
    parser.add_argument("--path2", type=str, help="Path to the financial data CSV file")
    parser.add_argument("--countries", type=str, help="Comma-separated list of countries")

    args = parser.parse_args()

    if not all([args.path1, args.path2, args.countries]):
        parser.print_help()
        exit(1)

    main(args)
    sys.exit()