# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import os
from settings import DATA_DIRECTORY

from pyspark.sql import SparkSession

spark = (
    SparkSession
        .builder
        .appName("Spark Data Sources")
        .config("spark.driver.memory", "8g")
        .config(
            "spark.driver.extraLibraryPath",
            os.path.join(os.environ["HOME"], "hadoop", "hadoop-2.10.1", "lib", "native")
        )
        .getOrCreate()

)

# %%
DIR = os.path.join(DATA_DIRECTORY, "flights", "summary-data")

parquet_file = os.path.join(DIR, "parquet/2010-summary.parquet")
json_file = os.path.join(DIR, "json/*")
csv_file = os.path.join(DIR, "csv/*")
orc_file = os.path.join(DIR, "orc/*")

# %%
schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

# %% [markdown]
# ## Parquet Data Source
# %%
df = (spark
      .read
      .format("parquet")
      .option("path", parquet_file)
      .load())

# %%
df2 = spark.read.parquet(parquet_file)

# %%
df.show(10, False)

# %%
# This will create an _unmanaged_ temporary view
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
    USING parquet
    OPTIONS (
      path '{parquet_file}'
    )
"""
)

# %%
spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# %%
# In Python
(df.write.format("parquet")
  .mode("overwrite")
  .option("compression", "snappy")
  .save("/tmp/data/parquet/df_parquet"))

# %% [markdown]
# ## JSON Data Source
# %%
df = spark.read.format("json").option("path", json_file).load()

# %%
df.show(10, truncate=False)

# %%
df2 = spark.read.json(json_file)

# %%
df2.show(10, False)

# %%
# This will create an _unmanaged_ temporary view
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
    USING json
    OPTIONS (
      path '{json_file}'
    )
"""
)

# %%
spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# %%
(df.write.format("json")
    .mode("overwrite")
    .option("compression", "snappy")
    .option("spark.io.compression.codec", "snappy")
    .save("/tmp/data/json/df_json"))

# %% [markdown]
# ## CSV Data Source
# %%
df = (spark
      .read
	 .format("csv")
	 .option("header", "true")
	 .schema(schema)
	 .option("mode", "FAILFAST")  # exit if any errors
	 .option("nullValue", "")	  # replace any null data field with “”
	 .option("path", csv_file)
	 .load())

# %%
df.show(10, truncate = False)

# %%
df.write.format("csv").mode("overwrite").save("/tmp/data/csv/df_csv")

# %%
(df.write.format("parquet")
  .mode("overwrite")
  .option("path", "/tmp/data/parquet/df_parquet")
  .option("compression", "snappy")
  .save())

# %%
%sx ls '/tmp/data/parquet/df_parquet'

# %%
df2 = (spark
       .read
       .option("header", "true")
       .option("mode", "FAILFAST")	 # exit if any errors
       .option("nullValue", "")
       .schema(schema)
       .csv(csv_file))

# %%
df2.show(10, truncate=False)

# %%
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
    USING csv
    OPTIONS (
        path '{csv_file}',
        header 'true',
        inferSchema 'true',
        mode 'FAILFAST'
    )
"""
)

# %%
spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# %% [markdown]
# ## ORC Data Source
# %%
df = (spark.read
      .format("orc")
      .option("path", orc_file)
      .load())

# %%
df.show(10, truncate=False)

# %%
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
    USING orc
    OPTIONS (
      path '{orc_file}'
    )
"""
)

# %%
spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# %%
(df.write.format("orc")
    .mode("overwrite")
    .option("compression", "snappy")
    .save("/tmp/data/orc/flights_orc"))

# %% [markdown]
# ## Image
# %%
from pyspark.ml import image

image_dir = os.path.join(DATA_DIRECTORY, "cctvVideos", "train_images")
images_df = spark.read.format("image").load(image_dir)

images_df.printSchema()

# %%
images_df.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(5, truncate=False)

# %% [markdown]
# ## Binary
# %%
path = os.path.join(DATA_DIRECTORY, "cctvVideos", "train_images")
binary_files_df = (spark.read.format("binaryFile")
  .option("pathGlobFilter", "*.jpg")
  .load(path))

binary_files_df.show(5)

# %% [markdown]
# To ignore any partitioning data discovery in a directory, you can set the `recursiveFileLookup` to `true`.
# %%
binary_files_df = (spark.read.format("binaryFile")
   .option("pathGlobFilter", "*.jpg")
   .option("recursiveFileLookup", "true")
   .load(path))

binary_files_df.show(5)
