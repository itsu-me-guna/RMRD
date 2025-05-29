from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging.config
logging.config.fileConfig('Properties/Configuration/logging.config')
logger = logging.getLogger('Get_spark')

def get_spark_object(env, appName):
    try:
        logger.info('get_spark_object spark object started')
        if env=='DEV':
            master='local'
        else:
            master='Yarn'
        logger.info('master is {}'.format(master))
        spark=SparkSession.builder.master(master).appName(appName).getOrCreate()
    except Exception as exp:
        logger.error('An error occured in the get_spark_object==='+str(exp))
    else:
        logger.info('Spark object create...')
    return spark
