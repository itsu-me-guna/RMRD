import os
from pyspark.sql import SparkSession
os.environ['JAVA_HOME'] = r'C:\Users\Admin\.jdks\corretto-18.0.2'

spark = SparkSession.builder.getOrCreate()
multiline = 'True'
dir = "C:\Users\Admin\IdeaProjects\CCRMRD\Source\SampleFile\2_Thalaivasal CC_1614_2024_01_01.json"
df = spark.read.format('json').option(multiline=multiline).load(dir)
df.show()

# df = spark.read.format('json').option(multiline=True).load('C:\Users\Admin\IdeaProjects\CCRMRD\Source\SampleFile\2_Thalaivasal CC_1614_2024_01_01.json')
# df.show()

# df = spark.read.format('json').option(multiline=multiline).load(dir)
# df.show()
