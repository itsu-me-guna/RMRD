import  logging.config
logging.config.fileConfig('Properties/Configuration/logging.config')
loggers=logging.getLogger('Validate')
def get_current_date(spark):
    try:
        loggers.warning('Started the get_current_date method...')
        cr_dt=spark.sql('select current_date')
        loggers.warning("current date by using spark is " + str(cr_dt.collect()))
    except Exception as ext:
        logging.error('An Error occured in get_current_date please check the trace===',str(exp))
        raise
    else:
        loggers.warning('Validation Done, go forward....')

def print_schema(df, df_name):
    try:
        loggers.warning("Printing dataframe schema {}".format(df_name))
        sch = df.schema.fields
        for i in sch:
            loggers.info(f"\{i}")
    except Exception as exp:
        loggers.error("An error occured during print_schema===", str(exp))
        raise
    else:
        loggers.info("Print schema done, go fwd...")


