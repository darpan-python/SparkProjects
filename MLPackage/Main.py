from pyspark.sql import SparkSession
import findspark
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import corr
from pyspark.sql.types import *
import pandas
from pyspark.ml.regression import LinearRegression
findspark.init("/home/darpan/spark-2.4.0-bin-hadoop2.7")

class sparkML:

    def __init__(self):
        self.schema = ""
        self.outputdir = '/home/darpan/Desktop/python-kafka-avro-master/mycsv.csv'
        self.sparkModule = SparkSession.builder.appName('sparkML').getOrCreate()

        self.df = self.sparkModule.read.format('CSV') \
            .load('/home/darpan/Desktop/python-kafka-avro-master/HURdat_ExtremeWeatherEvents.csv', header='true',
                  inferSchema='true')
        self.customSchema()
        pass

    def customSchema(self):
        self.df = self.df.withColumn("persistence", self.df["persistence"].cast(IntegerType()))
        self.df = self.df.withColumn("product", self.df["product"].cast(IntegerType()))
        self.df = self.df.withColumn("InitialMax", self.df["InitialMax"].cast(IntegerType()))
        self.df = self.df.withColumn("speed", self.df["speed"].cast(DoubleType()))
        self.df = self.df.withColumn("speed_z", self.df["speed_z"].cast(DoubleType()))
        self.df = self.df.withColumn("speed_m", self.df["speed_m"].cast(DoubleType()))
        self.df = self.df.withColumn("MaximumWind_p", self.df['MaximumWind_p'].cast(IntegerType()))
        self.df = self.df.withColumn("Latitude_p", self.df["Latitude_p"].cast(IntegerType()))
        self.df = self.df.withColumn("Longitude_p", self.df["Longitude_p"].cast(IntegerType()))
        self.df = self.df.withColumn("rapid_int", self.df["rapid_int"].cast(IntegerType()))
        self.df = self.df.withColumn("diff", self.df["diff"].cast(IntegerType()))
        self.df = self.df.drop('_c0')
        self.df = self.df.dropna()
        indexer = StringIndexer(inputCol='Status', outputCol='label')
        self.df = indexer.fit(self.df).transform(self.df)
        pass

    def main(self):
        assembler = VectorAssembler(inputCols=[
                                                    'Latitude','InitialMax',
                                                    'Longitude','MaximumWind',
                                                    'diff','MaximumWind_p',
                                                    'rapid_int', 'i', 'n',
                                                    'persistence', 'product',
                                                    'InitialMax', 'speed',
                                                    'speed_z', 'speed_m', 'Jday',
                                                    'Latitude_p', 'Longitude_p'], outputCol='features')

        self.df = assembler.transform(self.df)
        self.finaldata = self.df.select('*')

        train_data, test_data = self.finaldata.randomSplit([0.7, 0.3])

        #self.linearRegressionModel(train_data, test_data)
        self.RandomForest(train_data, test_data)
        #self.LogisticRegressionModel(train_data)

        pass

    def linearRegressionModel(self, train_data, test_data):
        lr = LinearRegression(labelCol='label', featuresCol='features')
        lr_model = lr.fit(train_data)

        testresults = lr_model.evaluate(test_data)
        test_data = test_data.select('features')
        prediction = lr_model.transform(test_data)
        prediction.select('*').show()
        print("RootMeanSqaure : " + str(testresults.rootMeanSquaredError))
        print("R2 : " + str(testresults.r2))
        print("Coefficients: " + str(lr_model.coefficients))
        print("Intercept: " + str(lr_model.intercept))
        pass

    def RandomForest(self, train_data, test_data):
        from pyspark.ml.classification import RandomForestClassifier
        rfc = RandomForestClassifier(labelCol='label', featuresCol='features', numTrees=100)
        rfc_model = rfc.fit(train_data)

        prediction = rfc_model.transform(test_data)
        from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

        my_binary_eval = BinaryClassificationEvaluator(labelCol='label')
        print('RFC')
        print(my_binary_eval.evaluate(prediction))

        my_mul_eval = MulticlassClassificationEvaluator(metricName='accuracy', labelCol='label')

        print(my_mul_eval.evaluate(prediction))
        frame = prediction.select('*')
        frame.toPandas().to_csv(self.outputdir)
        pass

    def LogisticRegressionModel(self, train_data):
        from pyspark.ml.classification import LogisticRegression
        lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

        # Fit the model
        lrModel = lr.fit(train_data)
        print("Coefficients: \n" + str(lrModel.coefficientMatrix))
        print("Intercept: " + str(lrModel.interceptVector))
    pass


test=sparkML()
test.main()


