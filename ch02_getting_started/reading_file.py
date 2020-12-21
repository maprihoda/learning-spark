# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
from settings import DATA_DIRECTORY

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# %%
import os
import wget

url = "https://raw.githubusercontent.com/apache/spark/master/README.md"
wget.download(url, out=DATA_DIRECTORY)

# %%

textFile = spark.read.text(os.path.join(DATA_DIRECTORY, "README.md"))

textFile.show(10, False)

# %%
textFile.count()

# %%
textFile.first()

# %%
linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
linesWithSpark.printSchema()

# %%
textFile.filter(textFile.value.contains("Spark")).count()

# %%
from pyspark.sql.functions import size, split

df = spark.createDataFrame(
    [["aaa bbb"], ["eee"], [""], ["\n"]], ['data']
)

df.printSchema()

# %%
df.filter(df.data != "").show()
df.filter(df.data != "").count()

# %%
# newline separator handled correctly
df.filter(df.data != "").select(split(df.data, "\s+")).show()

# %%
df.filter(df.data != "").select(size(split(df.data, "\s+"))).show()

# %%
from pyspark.sql.functions import col, max

# filter out empty rows
textFile.filter(textFile.value != "").show(5)

# %%
(textFile
    .filter(textFile.value != "")
    .select(split(textFile.value, "\s+").name("numWords")).show(5))

# %%

(textFile
    .filter(textFile.value != "")
    .select(size(split(textFile.value, "\s+")).name("numWords")).show(5))

# %%
word_counts_df = (
    textFile
        .filter(textFile.value != "")
        .select(
            size(split(textFile.value, "\s+")).name("numWords")
        )
        .agg(max(col("numWords")).alias("max_numwords"))
)

word_counts_df.printSchema()

# %%
word_counts_df.select("max_numwords").show()

# %%
# using explode
from pyspark.sql.functions import explode

word_counts_df = (
    textFile
        .filter(textFile.value != "")
        .select(
            explode(split(textFile.value, "\s+")).alias("word")
        )
        .groupBy("word")
        .count()
)

word_counts_df.printSchema()

# %%
word_counts_df.show(5)
