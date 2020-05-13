from pyspark.sql.types import *


class customSchema:

    def schema(self):
        # Define schema of json
        jsonSchema = StructType([StructField("NM", StringType(), True),
                                 StructField("CDI", StringType(), True),
                                 StructField("AMT", StringType(), True),
                                 StructField("DAN", StringType(), True),
                                 StructField("DSN", StringType(), True),
                                 StructField("CAN", StringType(), True),
                                 StructField("CSN", StringType(), True),
                                 StructField("DT", StringType(), True)
                                 ])
        return jsonSchema
