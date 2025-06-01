from pyspark.sql.functions import *
from pyspark.sql.types import *

@udf(returnType=IntegerType())

def columnSplit(clm):
    return len(clm.split(' '))