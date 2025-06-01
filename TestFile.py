import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from udfs import *
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

df = (df.withColumn("MilkType",regexp_replace(col("MilkType"),r"^C","Cow Milk"))
      .withColumn("TestParamName",regexp_replace(col("TestParamName"),r"^CONDUCT","Conduct"))
      .withColumn("TestParamName",regexp_replace(col("TestParamName"),r"^DENSITY","Density"))
      .withColumn("TestParamName",regexp_replace(col("TestParamName"),r"^FAT","Fat"))
      .withColumn("TestParamName",regexp_replace(col("TestParamName"),r"^PROTEIN","Protein"))
      .withColumn("TestParamName",regexp_replace(col("TestParamName"),r"^TEMP","Temp")))

# df.show()
# df.printSchema()
# print(df.schema.fields)
# print(df.schema.names)

# sch = df.schema.fields
# print(sch, type(sch))
# for i in sch:
#     print(i, f"\{i}")

# print(df.columns)

# df_nan = df.filter(isnan(col("EditStauts")))
# df_nan.show()

# df.select([count(when (isnan(col(c)) | col(c).isNull(), c).alias(c)) for c in df.columns]).show()
# df.select([count(when (col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# for c in df.columns:
#     print(c)


# df = spark.read.format('json').option(multiline=True).load('C:\Users\Admin\IdeaProjects\CCRMRD\Source\SampleFile\2_Thalaivasal CC_1614_2024_01_01.json')
# df.show()

# df = spark.read.format('json').option(multiline=multiline).load(dir)
# df.show()
# df.filter(col("val")==" ").show()

# df.filter(col("Val").isNotNull() & col("Val").cast("double").isNull()).show()

# df.printSchema()
# df.select(col("Val").cast(DecimalType(18,2))).show()

# df.filter(~col("Val").rlike("^[+-]?\\d*\\.?\\d+$")).show()
df = df.filter(col("Val").rlike("^-?\\d*\\.?\\d+$"))
# df.show()
df = df.na.fill(0,subset=["Val"])

# df = df.filter(col("Val").rlike("^[+-]?\\d*\\.?\\d+$"))
df = df.withColumn("SampleDate",columnSplit(col("SampleDate")))
df.show()

df = df.withColumn("Val",col("Val").cast(DecimalType(10, 2)))
# df.show()
df.groupBy("ShiftTiming","ShiftDesc","AnalyserType").pivot("TestParamName").max("Val").show()
