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
    except Exception as exp:
        loggers.error('An error occured duting the Data_transform===',str(exp))
        raise
    else:
        loggers.warning('Data_transform completed successfully...')
    return df

