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