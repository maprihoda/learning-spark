# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import os
from settings import DATA_DIRECTORY

from pyspark.sql import SparkSession

spark = (
    SparkSession
        .builder
        .appName("Spark Tables")
        .config("spark.driver.memory", "8g")
        .config(
            "spark.sql.catalogImplementation",
            "hive"
        )
        .getOrCreate()
)

us_flights_file = os.path.join(
    DATA_DIRECTORY, "flights", "departuredelays.csv"
)

# %% [markdown]
# ### Create Managed Tables
# %%
# Create database and managed tables
spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

spark.sql("""
CREATE TABLE us_delay_flights_tbl(
    date STRING,
    delay INT,
    distance INT,
    origin STRING,
    destination STRING
)""")

# %%
display(spark.catalog.listDatabases())

# %%
df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .option("path", us_flights_file)
      .load())

# %%
df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")

# %%
spark.sql("CACHE TABLE us_delay_flights_tbl")

# %%
spark.catalog.isCached("us_delay_flights_tbl")

# %% [markdown]
# ### Display tables within a Database
#
# Note that the table is MANAGED by Spark
# %%
spark.catalog.listTables(dbName="learn_spark_db")

# %% [markdown]
# ### Display Columns for a table
# %%
spark.catalog.listColumns("us_delay_flights_tbl")

# %% [markdown]
# ### Create Unmanaged Tables
# %%
# Drop the database and create unmanaged tables
spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

spark.sql(f"""
CREATE TABLE us_delay_flights_tbl (
    date STRING,
    delay INT,
    distance INT,
    origin STRING,
    destination STRING
) USING csv OPTIONS (path '{us_flights_file}')
""")

# %% [markdown]
# ### Display Tables
#
# **Note**: The table type here that tableType='EXTERNAL', which indicates it's unmanaged by Spark, whereas above the tableType='MANAGED'
# %%
spark.catalog.listTables(dbName="learn_spark_db")

# %%
spark.catalog.listColumns("us_delay_flights_tbl")

# %%
# %% [markdown]
# ### Create Unmanaged Tables using DataFrame API
# %%
# default is Parquet
(df
    .write
    .mode("overwrite")
    .option("path", "/tmp/data/us_flights_delay")
    .saveAsTable("us_delay_flights_tbl"))
