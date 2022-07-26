import pyspark
from pyspark import SparkContext

sc = SparkContext('local[*]')

x = sc.parallelize(["This", " is", " a", " test", " to", " execute", " Spark", " job", "with", "Airflow."])
y = x.reduce(lambda a, b: a + b)
y.saveAsTextFile("s3://kc-playground-s3-training/airflow/output/sparkjob_output.txt")
print(y)

