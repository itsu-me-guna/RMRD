from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging.config
logging.config.fileConfig('Properties/Configuration/logging.config')
loggers = logging.getLogger('Data_transform')

def data_clean(df, df_name):
    try:
        loggers.warning('Flattening the json file {}'.format(df_name))
        df = df.withColumn("Sample",explode(col("Sample"))).withColumn("Analyser", explode(col("Sample.Analyser"))).withColumn("TestRslt",explode(col("Analyser.TestRslt")))
        df = df.select("Sample.ApprovalStatus", "Sample.CCId", "Sample.CentreSampleNo", "Sample.ChannelNo", "Sample.EditStauts", "Sample.MilkType", "Sample.NewStatus", "Sample.ProcessName", "Sample.SampleChSrc", "Sample.SampleDate", "Sample.SampleNo", "Sample.ShiftDesc", "Sample.ShiftTiming", "Sample.TrayNo",
                       "Analyser.AnalyserName", "Analyser.AnalyserType", "TestRslt.DispName", "TestRslt.TestParamName", "TestRslt.TstParamAnsMapId", "TestRslt.TstParamId", "TestRslt.Val")

        loggers.warning('Replacing MilkType from C to Cow Milk')
        df = (df.withColumn("MilkType",regexp_replace(col("MilkType"),r"^C","Cow Milk"))
              .withColumn("TestParamName",regexp_replace(col("TestParamName"),r"^CONDUCT","Conduct"))
              .withColumn("TestParamName",regexp_replace(col("TestParamName"),r"^DENSITY","Density"))
              .withColumn("TestParamName",regexp_replace(col("TestParamName"),r"^FAT","Fat"))
              .withColumn("TestParamName",regexp_replace(col("TestParamName"),r"^PROTEIN","Protein"))
              .withColumn("TestParamName",regexp_replace(col("TestParamName"),r"^TEMP","Temp")))

        loggers.warning('Filter Invalid records')
        df = df.filter(col("Val").rlike("^[+-]?\\d*\\.?\\d+$"))

        loggers.warning('Checking Null values in the dataframe')
        df_na = df.select([count(when (col(c).isNull(), c)).alias(c) for c in df.columns])
        loggers.warning('Dropping Null Values')
        df = df.dropna(subset="EditStauts")
        df = df.dropna(subset="TstParamAnsMapId")
        loggers.warning('Successfully dropped Null values')

        loggers.warning('Convert Value into decimal')
        df = df.withColumn("Val", col("Val").cast(DecimalType(18,2)))

    except Exception as exp:
        loggers.error('An error occured duting the Data_transform===',str(exp))
        raise
    else:
        loggers.warning('Data_transform completed successfully...')
    return df

