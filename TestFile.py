import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
os.environ['JAVA_HOME'] = r'C:\Users\Admin\.jdks\corretto-18.0.2'

spark = SparkSession.builder.getOrCreate()
multiline = 'True'
dir = r"C:\Users\Admin\IdeaProjects\CCRMRD\Source\SampleFile\2_Thalaivasal CC_1614_2024_01_01.json"
df = spark.read.format('json').option('multiline', multiline).load(dir)
# df.show()
# df.printSchema()

df = df.withColumn("Sample",explode(col("Sample"))).withColumn("Analyser", explode(col("Sample.Analyser"))).withColumn("TestRslt",explode(col("Analyser.TestRslt")))

df = df.select("Sample.ApprovalStatus", "Sample.CCId", "Sample.CentreSampleNo", "Sample.ChannelNo", "Sample.EditStauts", "Sample.MilkType", "Sample.NewStatus",
"Sample.ProcessName",
"Sample.SampleChSrc",
"Sample.SampleDate",
"Sample.SampleNo",
"Sample.ShiftDesc",
"Sample.ShiftTiming",
"Sample.TrayNo",
"Analyser.AnalyserName",
"Analyser.AnalyserType",
"TestRslt.DispName",
"TestRslt.TestParamName",
"TestRslt.TstParamAnsMapId",
"TestRslt.TstParamId",
"TestRslt.Val")


# df.show()
df.printSchema()
# print(df.schema.fields)
# print(df.schema.names)

sch = df.schema.fields
print(sch, type(sch))
for i in sch:
    print(i, f"\{i}")


# df = spark.read.format('json').option(multiline=True).load('C:\Users\Admin\IdeaProjects\CCRMRD\Source\SampleFile\2_Thalaivasal CC_1614_2024_01_01.json')
# df.show()

# df = spark.read.format('json').option(multiline=multiline).load(dir)
# df.show()
