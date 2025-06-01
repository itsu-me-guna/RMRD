from pyspark.sql.functions import *
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
        df = df.withColumn("MilkType",regexp_replace(col("MilkType"),r"^C","Cow Milk"))

        loggers.warning('Checking Null values in the dataframe')
        df_na = df.select([count(when (col(c).isNull(), c)).alias(c) for c in df.columns])
        loggers.warning('Dropping Null Values')
        df = df.dropna(subset="EditStauts")
        df = df.dropna(subset="TstParamAnsMapId")
        loggers.warning('Successfully dropped Null values')

    except Exception as exp:
        loggers.error('An error occured duting the Data_transform===',str(exp))
        raise
    else:
        loggers.warning('Data_transform completed successfully...')
    return df

