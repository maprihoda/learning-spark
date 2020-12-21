# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = (
    SparkSession
        .builder
        .appName("Vectorized User Defined Functions")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
)

# %%
df = (spark
      .range(0, 10 * 1000 * 1000)
      .withColumn('id', (F.col('id') / 1000).cast('integer'))
      .withColumn('v', F.rand()))

df.printSchema()
df.show(5)

# %%
df.cache()
df.count()

# %% [markdown]
# ## Incrementing a column by one
# %% [markdown]
# ### PySpark UDF
# %%
@F.udf("double")
def plus_one(v):
    return v + 1

%timeit -n1 -r1 df.withColumn('v', plus_one(df.v)).agg(F.count(F.col('v'))).show()

# %% [markdown]
# Alternate Syntax (can also use in the SQL namespace now)
# %%
def plus_one(v):
    return v + 1

spark.udf.register("plus_one_udf", plus_one, T.DoubleType())

# %%
%timeit -n1 -r1 df.selectExpr("id", "plus_one_udf(v) as v").agg(F.count(F.col('v'))).show()

# %%
df.createOrReplaceTempView("df")

# %%
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

%timeit -n1 -r1 df.toPandas()

# %%

spark.conf.set("spark.sql.execution.arrow.enabled", "false")

%timeit -n1 -r1 df.toPandas()

# %% [markdown]
# ### Vectorized UDF
# %%
@F.pandas_udf('double')
def vectorized_plus_one(v):
    return v + 1

%timeit -n1 -r1 df.withColumn('v', vectorized_plus_one(df.v)).agg(F.count(F.col('v'))).show()

# %%
def vectorized_plus_one(v):
    return v + 1

vectorized_plus_one_udf = F.pandas_udf(vectorized_plus_one, "double")

%timeit -n1 -r1 df.withColumn('v', vectorized_plus_one_udf(df.v)).agg(F.count(F.col('v'))).show()

# %%
%timeit -n1 -r1 df.withColumn('v', df.v + F.lit(1)).agg(F.count(F.col('v'))).show()

# %% [markdown]
# ### PySpark UDF
# %%
# from pyspark.sql import Row
# import pandas as pd

# # DON'T do this
# @F.udf(T.ArrayType(df.schema))
# def subtract_mean(rows):
#     vs = pd.Series([r.v for r in rows])
#     vs = vs - vs.mean()
#     return [Row(id=rows[i]['id'], v=float(vs[i])) for i in range(len(rows))]

# %timeit -n1 -r1 ( \
#     df.groupby('id') \
#         .agg(F.collect_list(F.struct(df['id'], df['v'])).alias('rows')) \
#         .withColumn('new_rows', subtract_mean(F.col('rows'))) \
#         .withColumn('new_row', F.explode(F.col('new_rows'))) \
#         .withColumn('id', F.col('new_row.id')) \
#         .withColumn('v', F.col('new_row.v')) \
#         .agg(F.count(F.col('v'))).show())

# %% [markdown]
# ### Vectorized UDF
# %%
from pyspark.sql.functions import PandasUDFType

@F.pandas_udf(df.schema, PandasUDFType.GROUPED_MAP)
def vectorized_subtract_mean(pdf):
    return pdf.assign(v=pdf.v - pdf.v.mean())

%timeit -n1 -r1 ( \
    df.groupby('id') \
        .apply(vectorized_subtract_mean) \
        .agg(F.count(F.col('v'))) \
        .show())
