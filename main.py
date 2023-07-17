# import modules
from spark_utils import SparkUtils
import sys,logging
from datetime import datetime

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
    sparkutils = SparkUtils("MyApp")
    logger.info("Starting spark application")

 
    #do something here
    logger.info("Client Dataset - Reading")
    df_client = sparkutils.read_csv_to_spark_dataframe(args.path1)
    logger.info("Client Dataset - Previewing")
    df_client.show()

    logger.info("Financial Dataset - Reading")
    df_Financial= sparkutils.read_csv_to_spark_dataframe(args.path)
    logger.info("Financial Dataset - Previewing")
    df_Financial.show(truncate=False)

    
    sparkutils.destroy()
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