# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os
from settings import DATA_DIRECTORY

from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

spark = (
    SparkSession
        .builder
        .appName("blogs")
        .getOrCreate()
)

# %%
schema = T.StructType(
    [
        T.StructField("Id", T.IntegerType(), False),
        T.StructField("First", T.StringType(), False),
        T.StructField("Last", T.StringType(), False),
        T.StructField("Url", T.StringType(), False),
        T.StructField("Published", T.StringType(), False),
        T.StructField("Hits", T.IntegerType(), False),
        T.StructField("Campaigns", T.ArrayType(T.StringType()), False)
   ]
)

# %%
blogs_df = spark.read.json(os.path.join(DATA_DIRECTORY, "blogs.json"), schema=schema)
blogs_df.printSchema()

# %%
blogs_df.show(2)

# %%
blogs_df = blogs_df.withColumn("Name", F.concat(F.col("First"), F.lit(" "), F.col("Last")))
blogs_df.show(10, False)

# %%
blogs_df.groupBy("Name").agg(F.avg("Hits").alias("Average_Hits")).show()

# %%
blogs_df.select(F.expr("Hits") * 2).show(2)
blogs_df.select(F.col("Hits") * 2).show(2)
blogs_df.select(F.expr("Hits * 2")).show(2)

# %%
blogs_df.withColumn("Big Hitter", (F.expr("Hits > 10000"))).show()

# %%
blogs_df.sort(F.col("Id").desc()).show()
blogs_df.orderBy(F.col("Id"), ascending=False).show()

# %%
from pyspark.sql import Row

schema = T.StructType(
    [
        T.StructField("Author", T.StringType(), False),
        T.StructField("State", T.StringType(), False)
    ]
)

rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, schema)
authors_df.show()
