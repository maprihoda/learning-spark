# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import os
from settings import DATA_DIRECTORY

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType
import pyspark.sql.functions as F

spark = (
    SparkSession
        .builder
        .appName("San Francisco Fire Calls")
        .getOrCreate()
)

# %%
%sx ls "{DATA_DIRECTORY}/sf-fire-calls.csv"

# %%
%sx head -n 1 "{DATA_DIRECTORY}/sf-fire-calls.csv"

# %%
sf_fire_file = os.path.join(DATA_DIRECTORY, "sf-fire-calls.csv")

# %%
fire_schema = StructType(
    [
        StructField('CallNumber', IntegerType(), True),
        StructField('UnitID', StringType(), True),
        StructField('IncidentNumber', IntegerType(), True),
        StructField('CallType', StringType(), True),
        StructField('CallDate', StringType(), True),
        StructField('WatchDate', StringType(), True),
        StructField('CallFinalDisposition', StringType(), True),
        StructField('AvailableDtTm', StringType(), True),
        StructField('Address', StringType(), True),
        StructField('City', StringType(), True),
        StructField('Zipcode', IntegerType(), True),
        StructField('Battalion', StringType(), True),
        StructField('StationArea', StringType(), True),
        StructField('Box', StringType(), True),
        StructField('OriginalPriority', StringType(), True),
        StructField('Priority', StringType(), True),
        StructField('FinalPriority', IntegerType(), True),
        StructField('ALSUnit', BooleanType(), True),
        StructField('CallTypeGroup', StringType(), True),
        StructField('NumAlarms', IntegerType(), True),
        StructField('UnitType', StringType(), True),
        StructField('UnitSequenceInCallDispatch', IntegerType(), True),
        StructField('FirePreventionDistrict', StringType(), True),
        StructField('SupervisorDistrict', StringType(), True),
        StructField('Neighborhood', StringType(), True),
        StructField('Location', StringType(), True),
        StructField('RowID', StringType(), True),
        StructField('Delay', FloatType(), True)
    ]
)

# %%
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
fire_df.cache()

# %%
fire_df.printSchema()

# %%
fire_df.count()

# %%
fire_df.limit(5).show(vertical=True)

# %%
few_fire_df = (fire_df
                .where(F.col("CallType") != "Medical Incident")
                .select("IncidentNumber", "AvailableDtTm", "CallType"))

few_fire_df.show(5, truncate=False)

# %% [markdown]
# ### How many distinct types of calls were made to the Fire Department?
# %%
(fire_df
    .where(F.col("CallType").isNotNull())
    .select(F.countDistinct("CallType").alias("DistinctCallTypes"))
    .show())

# %%
(fire_df
    .where(F.col("CallType").isNotNull())
    .agg(F.countDistinct("CallType").alias("DistinctCallTypes"))
    .show())

# %%
# select returns a new dataframe
(fire_df
    .select("CallType")
    .where(F.col("CallType").isNotNull())
    .distinct()
    .count())

# %% [markdown]
# ### What are the distinct types of calls that were made to the Fire Department?
# %%
(fire_df
    .select("CallType")
    .where(F.col("CallType").isNotNull())
    .distinct()
    .show(100, False))

# %% [markdown]
# ### Find out all calls where the response time to the fire site was delayed for more than 5 mins
# %%
fire_df.select("CallNumber", "Delay").where(F.col("Delay") > 5).show(5, False)

# %% [markdown]
# ### Transform the string dates to Spark Timestamp data type
# %%
fire_ts_df = (
    fire_df
        .withColumn("IncidentDate", F.to_timestamp(F.col("CallDate"), "MM/dd/yyyy"))
            .drop("CallDate")
        .withColumn("OnWatchDate", F.to_timestamp(F.col("WatchDate"), "MM/dd/yyyy"))
            .drop("WatchDate")
        .withColumn("AvailableDtTS", F.to_timestamp(F.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
            .drop("AvailableDtTm")
)

# %%
fire_ts_df.cache()
fire_ts_df.columns

# %%
fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False)

# %% [markdown]
# ### What were the most common call types?
# %%
(fire_ts_df
    .select("CallType")
    .where(F.col("CallType").isNotNull())
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show(n=10, truncate=False))

# %% [markdown]
# ### What zip codes accounted for the most common calls?
# %%
(fire_ts_df
    .select("CallType", "ZipCode")
    .where(F.col("CallType").isNotNull())
    .groupBy("CallType", "Zipcode")
    .count()
    .orderBy("count", ascending=False)
    .show(10, truncate=False))

# %% [markdown]
# ### What San Francisco neighborhoods are in the zip codes 94102 and 94103
# %%
(fire_ts_df
    .select("Neighborhood", "Zipcode")
    .where((F.col("Zipcode") == 94102) | (F.col("Zipcode") == 94103))
    .distinct()
    .show(10, truncate=False))

# %% [markdown]
# ### What was the sum of all calls [NumAlarms], as well as the average, min and max of the response times for the calls?
# %%
fire_ts_df.select(
    F.sum("NumAlarms").alias("Sum_Calls"), # NB: sum is a Python bulit-in function => F.sum
    F.avg("Delay").alias("Avg_Delay"),
    F.min("Delay").alias("Min_Delay"),
    F.max("Delay").alias("Max_Delay")
).show(vertical=True)

# %% [markdown]
# ### How many distinct years of data is in the CSV file?
# %%
(fire_ts_df
    .select(
        F.year('IncidentDate').alias("Year_IncidentDate")
    )
    .distinct()
    .orderBy("Year_IncidentDate")
    .show())

# %% [markdown]
# ### What week of the year in 2018 had the most fire calls [incidents]?
# %%
(fire_ts_df
    .where(F.year('IncidentDate') == 2018)
    .groupBy(F.weekofyear('IncidentDate').alias("Week_of_Year"))
    .count()
    .select("Week_of_Year", F.col("count").alias("Count_of_Incidents"))
    .orderBy('Count_of_Incidents', ascending=False)
    .show(1))

# %% [markdown]
# ### What neighborhoods in San Francisco had the worst response time in 2018?
# %%
# NB: where returns a new dataframe
(fire_ts_df
    .where(F.year("IncidentDate") == 2018)
    .groupBy("Neighborhood")
    .agg(F.max("Delay").alias("Delay"))
    .orderBy("Delay", ascending=False)
    .show(100, False))

# %% [markdown]
# ### How can we use Parquet files to store data and read it back?
# %%
out = os.path.join(DATA_DIRECTORY, "fireServiceParquet")
fire_ts_df.write.format("parquet").mode("overwrite").save(out)

# %% [markdown]
# ### How can we use Parquet SQL table to store data and read it back?
# %%
%sx rm -rf "spark-warehouse/fireservicecalls"

# saving only on my localhost
(fire_ts_df
    .write.format("parquet")
    .mode("overwrite")
    .saveAsTable("FireServiceCalls", mode="overwrite"))

# %%
spark.sql("CACHE TABLE FireServiceCalls")

# %%
spark.sql("SELECT * FROM FireServiceCalls LIMIT 10").show(5)

# %% [markdown]
# ### How can read data from Parquet file?
# %%
file_parquet_df = spark.read.format("parquet").load(out)

file_parquet_df.limit(10).show(2, vertical=True)

# %% [markdown]
# ### How many calls were logged in the last seven days
# %%
last_incident_date_df = (
    fire_ts_df
        .select("IncidentDate")
        .agg(F.max(F.col("IncidentDate")).alias("IncidentDate"))
)

last_incident_date_df

# %%

last_incident_date = last_incident_date_df.first()
last_incident_date = last_incident_date.IncidentDate
last_incident_date

# %%
from datetime import timedelta

(fire_ts_df
    .where(F.col("IncidentDate") > (last_incident_date - timedelta(days=7)))
    .agg(F.sum("NumAlarms").alias("Sum_Calls"))
    .show(100, False))
