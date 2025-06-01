import os
import sys
from platform import system

import env_variable as ev                              # Import Environmental variable file
from env_variable import inferSchema
from get_spark import get_spark_object                 # Import get_Spark_object from get_Spark to create spark object
from validate import get_current_date, print_schema    # Import get_current_date from validate to validate spark object
from ingest import load_files, display_df, df_count
from data_transform import data_clean
from business_transform import intrn_data

import logging
import logging.config

logging.config.fileConfig('Properties/Configuration/logging.config')

def main():
    global file_format, header, inferSchema, file_dir, multiline
    try:
        print("i am in main page driver.py")
        logging.info('i am in the main method..')
        # print(ev.header)                                   # Lets use env variable
        # print(ev.src_sample_file)

        logging.info('Calling Spark Object')
        spark = get_spark_object(ev.env, ev.appName)
        logging.info('Validating Spark object')
        get_current_date(spark)                             # Validate the spark object

        # print(os.listdir(ev.src_sample_file))
        for file in os.listdir(ev.src_sample_file):
            file_dir=ev.src_sample_file + '\\' + file
            print('File is ', file_dir)

            if file.endswith('.json'):
                file_format = 'json'
                header = 'NA'
                inferSchema = 'NA'
                multiline = ev.multiline
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = ev.header
                inferSchema = ev.inferSchema
                multiline = 'NA'
            elif file.endswith('parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
                multiline = 'NA'
        logging.info('Reading file which is of > {}'.format(file_format))

        sample_file = load_files(spark=spark, file_format=file_format, file_dir=file_dir, header=header, inferSchema=inferSchema, multiline=multiline)

        logging.info('Displaying the dataframe > {}'.format(sample_file))
        display_df(sample_file, 'Sample File')
        logging.info('Dataframe validation Count...')
        df_count(sample_file, 'Sample File')

        logging.info('Data processing json flattening process')
        df_sample_file = data_clean(sample_file, 'Sample File')
        logging.info('Displaying the Processed dataframe > {}'.format(df_sample_file))
        display_df(df_sample_file, 'Processed Sample File')

        logging.info('Schema validation for data frame > {}'.format(df_sample_file))
        print_schema(df_sample_file, 'df_sample_file')

        logging.info('Applying business logics for first level transform')
        df_sample_sliver = intrn_data(df_sample_file, 'Sample File')

        display_df(df_sample_sliver, 'Sample File')
        # df_sample_sliver.show()

    except Exception as exp:
        logging.error('An Error occured when calling main() please check the trace===',str(exp))
        # sys.exit(1)
if __name__ == '__main__':
    main()
    logging.info('Application Run')
