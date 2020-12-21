# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession
        .builder
        .appName("Configuring Spark")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
)

# %%
spark.sql("SET -v").select("key", "value").show(n=50, truncate=False)

# %%
spark.conf.get("spark.sql.shuffle.partitions")

# %%
spark.conf.set("spark.sql.shuffle.partitions", 5)
spark.conf.get("spark.sql.shuffle.partitions")

# %% [markdown]
# Any values or flags defined in spark-defaults.conf will be read first, followed by those supplied on the command line with spark- submit, and finally those set via SparkSession in the Spark application
