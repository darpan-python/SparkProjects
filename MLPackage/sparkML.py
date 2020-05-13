import os

# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/darpan/spark-2.4.0-bin-hadoop2.7"
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = 'python3.6'
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf
from pyspark.sql.types import *