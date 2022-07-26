import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("DisplayCustomMessage")\
        .getOrCreate()

    x = spark.sparkContext.parallelize(["This", " is", " a", " test", " to", " execute", " Spark", " job", " with", " Airflow."])
    y = x.reduce(lambda a, b: a + b)
    print(y)

    spark.stop()
