# spark-submit mnmcount.py /home/marek/data/learning-spark/mnm_dataset.csv

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount.py <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (
        SparkSession
            .builder
            .appName("PythonMnMCount")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")


    # get the M&M data set file name
    mnm_file = sys.argv[1]

    # read the file into a Spark DataFrame
    mnm_df = spark.read.csv(mnm_file, inferSchema=True, header=True)

    mnm_df.show(n=5, truncate=False)

    # aggregate count of all colors and groupBy state and color
    # orderBy descending order
    count_mnm_df = (
        mnm_df
            .select("State", "Color", "Count")
            .groupBy("State", "Color")
            # .agg(sum(col("Count")).alias("Count"))
            .agg(sum("Count").alias("Count"))
            .orderBy("Count", ascending=False)
    )

    # show all the resulting aggregation for all the dates and colors
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # find the aggregate count for California by filtering
    ca_count_mnm_df = (
        mnm_df
            # .select("*")
            .where(col("State") == 'CA')
            .groupBy("State", "Color")
            .agg(sum("Count").alias("Count"))
            .orderBy("Count", ascending=False)
    )

    # show the resulting aggregation for California
    ca_count_mnm_df.show(n=10, truncate=False)
