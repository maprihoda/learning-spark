# pipenv shell

# spark-submit \
#   --master local[4] \
#   SimpleApp.py


# spark-submit SimpleApp.py
# OR
# python SimpleApp.py

# from project root
# spark-submit ch02_getting_started/SimpleApp.py


import os
from pyspark.sql import SparkSession

from settings import DATA_DIRECTORY

# spark_home = os.environ["SPARK_HOME"]

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# logFile = os.path.join(spark_home, "README.md")
logFile = os.path.join(DATA_DIRECTORY, "README.md")

logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
