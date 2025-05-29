import os

os.environ['JAVA_HOME'] = r'C:\Users\Admin\.jdks\corretto-18.0.2'   # JAVA JDK coretto included
# os.environ['JAVA_HOME'] = r'C:\Users\Admin\.jdks\corretto-15.0.2'
os.environ['env'] = 'DEV'                   # Environment name of the project
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['multiline'] = 'True'

env = os.environ['env']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
multiline = os.environ['multiline']

appName = 'CCRMRD Project'
# print("Current path ", os.getcwd())       # To get current path of the project where we are working
current_path = os.getcwd()

src_master = current_path + '\Source\Master'
src_sample_file = current_path + '\Source\SampleFile'
