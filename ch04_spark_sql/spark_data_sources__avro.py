# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import os
from settings import DATA_DIRECTORY

from pyspark.sql import SparkSession


# https://spark.apache.org/docs/latest/sql-data-sources-avro.html
spark = (
    SparkSession
        .builder
        .appName("Spark Data Sources - Avro")
        .config("spark.driver.memory", "8g")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.0.1"
        )
        .getOrCreate()
)

# OK /home/marek/.ivy2/jars/org.apache.spark_spark-avro_2.12-3.0.1.jar

# %%
DIR = os.path.join(DATA_DIRECTORY, "flights", "summary-data")

avro_file = os.path.join(DIR, "avro/*")

# %%
schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

# %%
df = (spark.read
      .format("avro")
      .option("path", avro_file)
      .load())

# %%
df.show(10, truncate=False)

# %%
# This will create an _unmanaged_ temporary view
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
    USING avro
    OPTIONS (
      path '{avro_file}'
    )
"""
)

# %%
spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# %%
(df.write
    .format("avro")
    .mode("overwrite")
    .save("/tmp/data/avro/df_avro"))
