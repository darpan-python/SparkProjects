from pyspark.sql import SparkSession
import findspark

findspark.init("/home/darpan/spark-2.4.0-bin-hadoop2.7")

import os

# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/darpan/spark-2.4.0-bin-hadoop2.7"
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = 'python3.6'


class sparkIntializer:
    def __init__(self,topicname, appname):
        self.appname = appname
        self.localHost = 'localhost:9092'
        self.topicname = topicname
        self.startingOffsets = 'earliest'
        pass

    def load(self):
        spark = SparkSession.builder.appName("SparkKafkaConsumer").getOrCreate()
        stream = spark.readStream \
            .format('kafka') \
            .option("kafka.bootstrap.servers", self.localHost) \
            .option("subscribe", self.topicname) \
            .option("startingOffsets", self.startingOffsets) \
            .option("includeTimestamp", "true") \
            .load()
        return stream
