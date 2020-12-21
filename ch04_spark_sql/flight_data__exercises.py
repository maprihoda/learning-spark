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
        .appName("US departure flight data")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
)

# %%
df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .option("path", os.path.join(DATA_DIRECTORY, "flights", "departuredelays.csv"))
      .load())

# %%
df.show(5, False)

# %%
df = df.withColumn("date_iso", F.to_date(F.col("date"), "MMddHHmm"))

# %%
df.show(50, False)

# %%
# Assume the data are for 2019
df = df.withColumn("date_iso", F.add_months(F.col("date_iso"), 12 * 49))

# %%
df.show(50, False)

# %%
df.createOrReplaceTempView("departuredelays")

# %%
# subquery
spark.sql("""
SELECT date_iso, delay, origin, destination
FROM departuredelays
WHERE delay > 120
    AND ORIGIN = 'SFO'
    AND DESTINATION = 'ORD'
ORDER by delay DESC
"""
).show(10, truncate=False)

# %%
spark.sql("""
SELECT DISTINCT MONTH(date_iso)
FROM departuredelays
LIMIT 50
"""
).show(50, truncate=False)

# %%
spark.sql("""
WITH delayed_flights_sfo_ord
AS
(
    SELECT date_iso, delay, origin, destination
    FROM departuredelays
    WHERE delay > 120
        AND ORIGIN = 'SFO'
        AND DESTINATION = 'ORD'
    ORDER by delay DESC
)
SELECT
    YEAR(date_iso),
    MONTH(date_iso),
    COUNT(*) AS count_delays_gt_2hrs
FROM delayed_flights_sfo_ord
GROUP BY YEAR(date_iso),
         MONTH(date_iso)
ORDER BY 3 DESC;
"""
).show(10, truncate=False)

# %%
spark.sql("""
WITH delayed_flights_sfo_ord
AS
(
    SELECT date_iso, delay, origin, destination
    FROM departuredelays
    WHERE delay > 120
        AND ORIGIN = 'SFO'
        AND DESTINATION = 'ORD'
    ORDER by delay DESC
)
SELECT
    YEAR(date_iso),
    MONTH(date_iso),
    DAY(date_iso),
    COUNT(*) AS count_delays_gt_2hrs
FROM delayed_flights_sfo_ord
GROUP BY YEAR(date_iso),
         MONTH(date_iso),
         DAY(date_iso)
ORDER BY 4 DESC;
"""
).show(10, truncate=False)
