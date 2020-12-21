# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os
from settings import DATA_DIRECTORY

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.functions as T

spark = (
    SparkSession
        .builder
        .appName("US departure flight data")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
)

# %%
def to_date_format_udf(d_str: str):
    l = [char for char in d_str]
    return "".join(l[0:2]) + "/" + "".join(l[2:4]) + " " + " " + "".join(l[4:6]) + ":" + "".join(l[6:])

to_date_format_udf("02190925")

# %%
spark.udf.register("to_date_format_udf", to_date_format_udf, T.StringType())

# %%
df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .option("path", os.path.join(DATA_DIRECTORY, "flights", "departuredelays.csv"))
      .load())

# %%
df.show(5, False)

# %%
df.selectExpr("to_date_format_udf(date) as data_format").show(10, truncate=False)

# %%
df.createOrReplaceTempView("us_delay_flights_tbl")

# %%
spark.sql("CACHE TABLE us_delay_flights_tbl")

# %%
spark.sql("""
SELECT *, date, to_date_format_udf(date) AS date_fm
FROM us_delay_flights_tbl
"""
).show(10, truncate=False)

# %%
spark.sql("""
SELECT COUNT(*)
FROM us_delay_flights_tbl
"""
).show()

# %% [markdown]
# Find out all flights whose distance between origin and destination is greater than 1000
# %%
spark.sql("""
SELECT distance, origin, destination
FROM us_delay_flights_tbl
WHERE distance > 1000
ORDER BY distance DESC
"""
).show(10, truncate=False)

# %%
(df.select("distance", "origin", "destination")
    .where(F.col("distance") > 1000)
    .orderBy(F.desc("distance"))
    .show(10, truncate=False))

# %%
(df.select("distance", "origin", "destination")
    .where("distance > 1000")
    .orderBy("distance", ascending=False)
    .show(10))

# %%
(df.select("distance", "origin", "destination")
    .where("distance > 1000")
    .orderBy(F.desc("distance"))
    .show(10))

# %% [markdown]
# Find out all flights with 2 hour delays between San Francisco and Chicago
# %%
spark.sql("""
SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120
    AND ORIGIN = 'SFO'
    AND DESTINATION = 'ORD'
ORDER by delay DESC
"""
).show(10, truncate=False)

# %% [markdown]
# Label all US flights originating from airports with high, medium, low, no delays, regardless of destinations
# %%
spark.sql("""
SELECT
    delay,
    origin,
    destination,
    CASE
        WHEN delay > 360 THEN 'Very Long Delays'
        WHEN delay > 120 AND delay < 360 THEN  'Long Delays '
        WHEN delay > 60 AND delay < 120 THEN  'Short Delays'
        WHEN delay > 0 and delay < 60  THEN   'Tolerable Delays'
        WHEN delay = 0 THEN 'No Delays'
        ELSE 'No Delays'
    END AS Flight_Delays
FROM us_delay_flights_tbl
ORDER BY origin, delay DESC
"""
).show(10, truncate=False)

# %%
spark.sql("""
SELECT
    delay,
    origin,
    destination,
    CASE
        WHEN delay > 360 THEN 'Very Long Delays'
        WHEN delay > 120 THEN  'Long Delays '
        WHEN delay > 60 THEN  'Short Delays'
        WHEN delay > 0 THEN   'Tolerable Delays'
        WHEN delay = 0 THEN 'No Delays'
        ELSE 'No Delays'
    END AS Flight_Delays
FROM us_delay_flights_tbl
ORDER BY origin, delay ASC
"""
).show(10, truncate=False)

# %%
df1 = spark.sql("""
SELECT
    date,
    delay,
    origin,
    destination
FROM us_delay_flights_tbl
WHERE origin = 'SFO'
"""
)

# %%
df1.createOrReplaceGlobalTempView("us_origin_airport_SFO_tmp_view")

# %%
spark.sql(
"""
SELECT *
FROM global_temp.us_origin_airport_SFO_tmp_view
"""
).show(10, False)

# %%
spark.sql(
"""
DROP VIEW IF EXISTS global_temp.us_origin_airport_SFO_tmp_view
"""
)

# %%
spark.sql(
"""
DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view
"""
)

# %%
df2 = spark.sql("""
SELECT
    date,
    Delay,
    origin,
    destination
from us_delay_flights_tbl
WHERE origin = 'JFK'
"""
)

# %%
df2.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# %%
spark.sql("""
SELECT * FROM us_origin_airport_JFK_tmp_view
"""
).show(10, False)

# %%
spark.sql("""
DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view
"""
)

# %%
spark.catalog.listTables(dbName="global_temp")
