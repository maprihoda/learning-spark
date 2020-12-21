# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os
from settings import DATA_DIRECTORY

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession
        .builder
        .appName("Iot Devices")
        .getOrCreate()
)

# %%
iot_devices_file = os.path.join(DATA_DIRECTORY, "iot_devices.json")

df = spark.read.json(iot_devices_file)

# %%
df.printSchema()

# %%
df.show(5, False)

# %%
filtered_df = df.where(
    (F.col("temp") > 30) & (F.col("humidity") > 70)
)

# %%
filtered_df.show(5, False)

# %%
(df.select("temp", "device_name", "device_id", "cca3")
    .where("temp > 25")
    .show(5, False)
)

# %% [markdown]
# ### Detect failing devices with low battery below a threshold
# %%
(df.select("battery_level", "c02_level", "device_name")
    .where("battery_level < 8")
    .sort(F.col("c02_level").desc())
    .show(5, False))

# %% [markdown]
# ### Identify offending countries with high-levels of C02 emissions
# %%
(df.where(F.col("c02_level") > 1300)
  .groupBy(F.col("cn").alias("cn"))
  .agg(F.avg("c02_level").alias("avg_c02_level"))
  .sort(F.col("avg_c02_level").desc())
  .show(5, False)
)

# %% [markdown]
# ### Group the countries and sort them by average temperature and humidity
# %%
(df
    .where((F.col("temp") > 25) & (F.col("humidity") > 75))
    .select("cn", "temp", "humidity")
    .groupBy("cn")
    .agg(
        F.avg("temp").alias("avg_temp"),
        F.avg("humidity").alias("avg_humidity")
    )
    .sort(F.col("avg_temp").desc(), F.col("avg_humidity").desc() )
    .show(10, False))

# %%
# %% [markdown]
# ### Compute the min and max values for temperature, C02, and humidity
# %%
(df
    .select(
        F.min("temp"),
        F.max("temp"),
        F.min("humidity"),
        F.max("humidity"),
        F.min("c02_level"),
        F.max("c02_level"),
        F.min("battery_level"),
        F.max("battery_level"))
        .show(10))
