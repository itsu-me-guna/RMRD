import logging.config

logging.config.fileConfig('Properties/Configuration/logging.config')
logger = logging.getLogger('Ingest')

def load_files(spark, file_format, file_dir, header, inferSchema, multiline):
    try:
        logger.info('load_files method started...')
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'json':
            df = spark.read.format(file_format).option('multiline', multiline).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format).option(header=header).option(inferSchema=inferSchema).load(file_dir)
    except Exception as exp:
        logger.error('An error occured at load_files===', str(exp))
        raise
    else:
        logger.info('Dataframe created successfully which is {}'.format(file_format))

    return df

def display_df(df, dfname):
    df_show = df.show()
    return df_show
def df_count(df, dfname):
    df_cnt=df.count()
    logger.info('Dataframe validation count is {}'.format(df_cnt))
    return df_cnt

