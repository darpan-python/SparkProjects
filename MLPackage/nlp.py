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
        self.rfc_model = ''
        self.outputdir = '/home/darpan/Desktop/python-kafka-avro-master/mycsv.csv'
        self.modeldata = '/home/darpan/Desktop/python-kafka-avro-master/HURdat_ExtremeWeatherEvents.csv'
        self.sparkModule = SparkSession.builder.appName('sparkML').getOrCreate()
        pass

    def customSchema(self, df):

        df = df.withColumn("persistence", df["persistence"].cast(IntegerType()))
        df = df.withColumn("product", df["product"].cast(IntegerType()))
        df = df.withColumn("InitialMax", df["InitialMax"].cast(IntegerType()))
        df = df.withColumn("speed", df["speed"].cast(DoubleType()))
        df = df.withColumn("speed_z", df["speed_z"].cast(DoubleType()))
        df = df.withColumn("speed_m", df["speed_m"].cast(DoubleType()))
        df = df.withColumn("MaximumWind_p", df['MaximumWind_p'].cast(IntegerType()))
        df = df.withColumn("Latitude_p", df["Latitude_p"].cast(IntegerType()))
        df = df.withColumn("Longitude_p", df["Longitude_p"].cast(IntegerType()))
        df = df.withColumn("rapid_int", df["rapid_int"].cast(IntegerType()))
        df = df.withColumn("diff", df["diff"].cast(IntegerType()))
        df = df.drop('_c0')
        df = df.dropna()
        indexer = StringIndexer(inputCol='Status', outputCol='StatusInt')
        df = indexer.fit(df).transform(df)

        assembler = VectorAssembler(inputCols=[
            'Latitude', 'InitialMax',
            'Longitude', 'MaximumWind',
            'diff', 'MaximumWind_p',
            'StatusInt', 'i', 'n',
            'persistence', 'product',
            'InitialMax', 'speed',
            'speed_z', 'speed_m', 'Jday',
            'Latitude_p', 'Longitude_p'], outputCol='features')

        df = assembler.transform(df)
        return df

    def main(self):
        df = self.sparkModule.read.format('CSV') \
            .load(self.modeldata, header='true',
                  inferSchema='true')
        df = self.customSchema(df)
        finaldata = df.select('*')
        train_data, test_data = finaldata.randomSplit([0.7, 0.3])

        #self.linearRegressionModel(train_data, test_data)
        self.RandomForest(train_data, test_data)
        #self.LogisticRegressionModel(train_data)

        pass

    def linearRegressionModel(self, train_data, test_data):

        lr = LinearRegression(labelCol='rapid_int', featuresCol='features')
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
        rfc = RandomForestClassifier(labelCol='rapid_int', featuresCol='features', numTrees=100)
        rfc_model = rfc.fit(train_data)
        self.rfc_model = rfc_model
        test_data.show()
        prediction = rfc_model.transform(test_data)

        from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
        my_binary_eval = BinaryClassificationEvaluator(labelCol='rapid_int')
        print('RFC')
        print(my_binary_eval.evaluate(prediction))
        my_mul_eval = MulticlassClassificationEvaluator(metricName='accuracy', labelCol='rapid_int')
        print(my_mul_eval.evaluate(prediction))
        frame = prediction.select('*')
        frame.toPandas().to_csv(self.outputdir)
        pass

    def LogisticRegressionModel(self, train_data):
        from pyspark.ml.classification import LogisticRegression
        lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, labelCol='rapid_int')

        # Fit the model
        lrModel = lr.fit(train_data)
        print("Coefficients: \n" + str(lrModel.coefficientMatrix))
        print("Intercept: " + str(lrModel.interceptVector))
        pass

    def evaluatorData(self, filepath):
        test_data = self.sparkModule.read.format('CSV') \
            .load(filepath, header='true',
                  inferSchema='true')
        test_data = self.customSchema(test_data)
        df = self.rfc_model.transform(test_data)
        df.toPandas().to_csv(self.outputdir)

test=sparkML()
test.main()
test.evaluatorData('/home/darpan/Desktop/python-kafka-avro-master/HURdat_ExtremeWeatherEvents.csv')