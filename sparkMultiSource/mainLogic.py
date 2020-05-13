from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StringType
from sparkMultiSource.sparkintializer import sparkIntializer
from sparkMultiSource.customSchema import customSchema


class main:
    def flow(self):
        datframe = sparkIntializer('payments', 'SparkKafkaConsumer12')
        df = datframe.load()
        fschema = customSchema()
        js = fschema.schema()
        dataframe, value = self.transformation(df, js)
        self.spark_sink(dataframe)
        self.spark_sink1(dataframe)

    def transformation(self, datframe, jsonSchema):
        value = "Unisys"
        datframe = datframe.selectExpr(
            "CAST(value AS STRING)"
        )

        # pis service
        def pis(x, y, z):
            debit = int(x)
            credit = int(y)
            if z == 'Credit':

                if credit % 2 == 0:
                    return 'Unisys'
                else:
                    return 'Sap'
                pass
            else:
                if debit % 2 == 0:
                    return 'Unisys'
                else:
                    return 'Sap'

        datframe = datframe.select("value",
                                from_json(datframe["value"].cast("string"), jsonSchema).alias('person'))

        datframe = datframe.selectExpr("person.*")

        '''
        lines = lines.selectExpr("person.NM", "person.CDI", "person.AMT", "person.DAN", "person.DSN", "person.CAN", 
                                 "person.CSN", "person.DT")
                                 '''

        datframe = datframe.withColumnRenamed('NM', 'Name') \
            .withColumnRenamed('CDI', 'Indicator') \
            .withColumnRenamed('AMT', 'Amount') \
            .withColumnRenamed('DAN', 'DebitAccountNumber') \
            .withColumnRenamed('DSN', 'DebitSortCode') \
            .withColumnRenamed('CAN', 'CreditAccountNumber') \
            .withColumnRenamed('CSN', 'CreditSortCode') \
            .withColumnRenamed('DT', 'DateTime')
        datframe = datframe.dropna()

        pis_udf = udf(lambda x, y, z: pis(x, y, z), StringType())
        datframe = datframe.withColumn('Ledger', pis_udf(datframe.DebitSortCode, datframe.CreditSortCode, datframe.Indicator))
        ab = (datframe, value)
        return ab

    def spark_sink1(self, lines):
        query = lines \
            .writeStream \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="20 seconds") \
            .start() \
            .awaitTermination()
        pass
    def spark_sink(self, lines):
        query = lines \
            .writeStream \
            .outputMode("append") \
            .trigger(processingTime="50 seconds") \
            .format("CSV") \
            .option("path", "/home/darpan/snap1/Test") \
            .option("checkpointLocation", "/home/darpan/snap1") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="20 seconds") \
            .start() \
            .awaitTermination()
        pass
    pass


x = main()
x.flow()
