# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# ### User Defined Functions
# %%
import os
from settings import DATA_DIRECTORY

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = (
    SparkSession
        .builder
        .appName("Interacting with External Data Sources")
        .config("spark.driver.memory", "8g")
        .config(
            "spark.sql.catalogImplementation",
            "hive"
        )
        .getOrCreate()
)

# %%
# Create a cubed function
def cubed(s):
  return s * s * s

spark.udf.register("cubed", cubed, T.LongType())

spark.range(1, 9).createOrReplaceTempView("udf_test")

# %%
spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

# %% [markdown]
# ## Speeding up and Distributing PySpark UDFs with Pandas UDFs
# %%
import pandas as pd

def cubed(a: pd.Series) -> pd.Series:
    return a * a * a

x = pd.Series([1, 2, 3])

print(cubed(x))

# %%
cubed_udf = F.pandas_udf(cubed, returnType=T.LongType())

# %%
df = spark.range(1, 4)

df.select("id", cubed_udf(F.col("id"))).show()

# %% [markdown]
# ## Higher Order Functions in DataFrames and Spark SQL
# %%
arrayData = [[1, (1, 2, 3)], [2, (2, 3, 4)], [3, (3, 4, 5)]]

arraySchema = (
    T.StructType(
        [
            T.StructField("id", T.IntegerType(), True),
            T.StructField("values", T.ArrayType(T.IntegerType()), True)
        ]
    )
)

# df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
df = spark.createDataFrame(arrayData, arraySchema)
df.createOrReplaceTempView("table")
df.printSchema()
df.show()

# %% [markdown]
# #### Option 1: Explode and Collect
# %%
spark.sql("""
SELECT id, explode(values) AS value
      FROM table
""").show()

# %%
spark.sql("""
SELECT  id,
        collect_list(value + 1) AS newValues
FROM  (SELECT id, explode(values) AS value
      FROM table) x
GROUP BY id
""").show()

# %% [markdown]
# #### Option 2: User Defined Function
# %%
def addOne(values):
    return [value + 1 for value in values]

spark.udf.register("plusOneIntPy", addOne, T.ArrayType(T.IntegerType()))

spark.sql("SELECT id, plusOneIntPy(values) AS values FROM table").show()

# %% [markdown]
# ### Higher-Order Functions
# %%
schema = T.StructType([T.StructField("celsius", T.ArrayType(T.IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

t_c.show()

# %% [markdown]
# #### Transform
#
# `transform(array<T>, function<T, U>): array<U>`
# %%
# Calculate Fahrenheit from Celsius for an array of temperatures
spark.sql("""
    SELECT
        celsius,
        transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
    FROM tC
""").show()

# %% [markdown]
# #### Filter
#
# `filter(array<T>, function<T, Boolean>): array<T>`
# %%
# Filter temperatures > 38C for array of temperatures
spark.sql("""
SELECT
    celsius,
    filter(celsius, t -> t > 38) as high
FROM tC
""").show()

# %% [markdown]
# #### Exists
#
# `exists(array<T>, function<T, V, Boolean>): Boolean`
# %%
# Is there a temperature of 38C in the array of temperatures
spark.sql("""
SELECT
    celsius,
    exists(celsius, t -> t = 38) as threshold
FROM tC
""").show()

# %% [markdown]
# #### Aggregate (kinda Reduce)
# aggregate(expr, start_val, merge_fn, finish_fn)
# %%
# Calculate average temperature and convert to F
spark.sql("""
SELECT celsius,
    aggregate(
        celsius,
        0,
        (acc, t) -> acc + t,
        acc -> (acc div size(celsius) * 9 div 5) + 32
    ) as avgFahrenheit
FROM tC
""").show()

# %% [markdown]
# ## DataFrames and Spark SQL Common Relational Operators
# %%
delays_path = os.path.join(DATA_DIRECTORY, "flights", "departuredelays.csv")
airports_path = os.path.join(DATA_DIRECTORY, "flights", "airport-codes-na.txt")

airports = spark.read.options(header='true', inferSchema='true', sep='\t').csv(airports_path)
airports.createOrReplaceTempView("airports_na")

delays = spark.read.options(header='true').csv(delays_path)
delays = (delays
          .withColumn("delay", F.expr("CAST(delay as INT) as delay"))
          .withColumn("distance", F.expr("CAST(distance as INT) as distance")))

delays.createOrReplaceTempView("departureDelays")

# Create temporary small table
foo = delays.where(
    F.expr("""
        origin == 'SEA' AND
        destination == 'SFO' AND
        date like '01010%' AND
        delay > 0
    """)
)

foo.createOrReplaceTempView("foo")

# %%
spark.sql("SELECT * FROM airports_na LIMIT 10").show()

# %%
spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

# %%
spark.sql("SELECT * FROM foo LIMIT 10").show()

# %% [markdown]
# ## Unions (UNION ALL)
# %%
bar = delays.union(foo)

bar.createOrReplaceTempView("bar")

bar.where(
    F.expr("origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0")
).show()

# %%
spark.sql("""
SELECT DISTINCT *
FROM bar
WHERE origin = 'SEA'
    AND destination = 'SFO'
    AND date LIKE '01010%'
    AND delay > 0
""").show()

# %% [markdown]
# ## Joins
# %%
# Join Departure Delays data (foo) with flight info
foo.join(
    airports,
    airports.IATA == foo.origin
).select("City", "State", "date", "delay", "distance", "destination").show()

# %%
spark.sql("""
SELECT
    a.City,
    a.State,
    f.date,
    f.delay,
    f.distance,
    f.destination
FROM foo f
    JOIN airports_na a
    ON a.IATA = f.origin
""").show()

# %%
spark.sql("""
SELECT
    a1.City AS OriginCity,
    a1.State AS OriginState,
    f.date,
    f.delay,
    f.distance,
    f.destination,
    a2.City AS DestinationCity
FROM foo f
    JOIN airports_na a1
        ON a1.IATA = f.origin
    JOIN airports_na a2
        ON a2.IATA = f.destination
""").show()

# %% [markdown]
# ## Window Functions
# %%
spark.sql("DROP TABLE IF EXISTS departureDelaysWindow")

spark.sql("""
CREATE TABLE departureDelaysWindow AS
SELECT
    origin,
    destination,
    sum(delay) as TotalDelays
FROM departureDelays
WHERE origin IN ('SEA', 'SFO', 'JFK')
    AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
GROUP BY origin, destination
""")

spark.sql("""SELECT * FROM departureDelaysWindow""").show()

# %% [markdown]
# What are the top three routes with the highest total delays grouped by origin?
# %%
spark.sql("""
SELECT
    origin,
    destination,
    TotalDelays,
    rank
FROM (
    SELECT
        origin,
        destination,
        TotalDelays,
        dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
    FROM departureDelaysWindow
) t
WHERE rank <= 3
ORDER BY origin
""").show()

# %% [markdown]
# ### Adding New Columns
# %%
foo2 = foo.withColumn(
    "status",
    F.expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
)

foo2.show()

# %%
spark.sql("""
    SELECT
        *,
        CASE
            WHEN delay <= 10 THEN 'On-time'
            ELSE 'Delayed'
        END AS status
    FROM foo
""").show()

# %% [markdown]
# ### Dropping Columns
# %%
foo3 = foo2.drop("delay")
foo3.show()

# %% [markdown]
# ### Renaming Columns
# %%
foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

# %% [markdown]
# ### Pivoting
# %%
spark.sql("""
SELECT
    destination,
    date,
    CAST(SUBSTRING(date, 0, 2) AS int) AS month,
    delay
FROM departureDelays
WHERE origin = 'SEA'
""").show(10)

# %%
spark.sql("""
SELECT *
FROM (
    SELECT
        destination,
        CAST(SUBSTRING(date, 0, 2) AS int) AS month,
        delay
    FROM departureDelays
    WHERE origin = 'SEA'
)
PIVOT (
    CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay,
    MAX(delay) as MaxDelay
    -- FOR month IN (1, 2, 3)
    FOR month IN (1 JAN, 2 FEB, 3 MAR)
)
ORDER BY destination
""").show()

# %%
spark.sql("""
SELECT * FROM (
    SELECT
        destination,
        CAST(SUBSTRING(date, 0, 2) AS int) AS month,
        delay
    FROM departureDelays
    WHERE origin = 'SEA'
)
PIVOT (
    CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay,
    MAX(delay) as MaxDelay
    FOR month IN (1 JAN, 2 FEB)
)
ORDER BY destination
""").show()
