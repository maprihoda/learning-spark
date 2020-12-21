# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession
        .builder
        .appName("Caching Data")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
)

# %%
df = (
    spark.range(1 * 10_000_000)
        .toDF("id").
        withColumn("square", F.col("id") * F.col("id"))
)

df.cache().count()

# %%
df.count()

# %% [markdown]
# Check the Spark UI storage tab to see where the data is stored.
# http://localhost:4040/storage/
# %% [markdown]
# If you do not unpersist, df2 below will not be cached because it has the same query plan as df
# %%
df.unpersist()

# %% [markdown]
# ### Use _persist(StorageLevel.Level)_
# %%
from pyspark import StorageLevel

df2 = spark.range(1 * 10_000_000).toDF("id").withColumn("square", F.col("id") * F.col("id"))

df2.persist(StorageLevel.DISK_ONLY).count()

# %%
df2.count()

# %% [markdown]
# Check the Spark UI storage tab to see where the data is stored
# %%
df2.unpersist()

# %%
df.createOrReplaceTempView("dfTable")
spark.sql("CACHE TABLE dfTable")

# %% [markdown]
# Check the Spark UI storage tab to see where the data is stored.
# %%
spark.sql("SELECT count(*) FROM dfTable").show()
