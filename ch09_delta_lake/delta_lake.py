# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
# https://resources.lendingclub.com/LCDataDictionary.xlsx

import os
from settings import DATA_DIRECTORY

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = (
    SparkSession
        .builder
        .appName("Building Reliable Data Lakes with Delta Lake and Apache Spark")
        .config("spark.driver.memory", "8g")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
)

# %%
spark.sql("set spark.sql.shuffle.partitions = 1")

sourcePath = os.path.join(DATA_DIRECTORY, "loans", "loan-risks.snappy.parquet")

deltaPath = "/tmp/loans_delta"

%sx rm -rf {deltaPath}

(spark
    .read.format("parquet")
    .load(sourcePath)
    .write.format("delta")
    .save(deltaPath))

(spark
    .read
    .format("delta")
    .load(deltaPath)
    .createOrReplaceTempView("loans_delta"))

print("Defined view 'loans_delta'")

# %%
spark.sql("SELECT count(*) FROM loans_delta").show()

# %%
spark.sql("SELECT * FROM loans_delta LIMIT 5").show()

# %%
import random

def random_checkpoint_dir():
    return "/tmp/chkpt/%s" % str(random.randint(0, 10_000))

states = ["CA", "TX", "NY", "WA"]

@F.udf(returnType=T.StringType())
def random_state():
    return str(random.choice(states))

def generate_and_append_data_stream():
    newLoanStreamDF = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
        .withColumn("loan_id", 10_000 + F.col("value"))
        .withColumn("funded_amnt", (F.rand() * 5_000 + 5_000).cast("integer"))
        .withColumn("paid_amnt", F.col("funded_amnt") - (F.rand() * 2_000))
        .withColumn("addr_state", random_state())
        .select("loan_id", "funded_amnt", "paid_amnt", "addr_state"))

    checkpointDir = random_checkpoint_dir()

    streamingQuery = (newLoanStreamDF.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir)
        .trigger(processingTime = "10 seconds")
        .start(deltaPath))

    return streamingQuery

def stop_all_streams():
    import shutil

    print("Stopping all streams")
    for s in spark.streams.active:
        s.stop()
    print("Stopped all streams")

    print("Deleting checkpoints")
    shutil.rmtree(os.path.join("/", "tmp", "chkpt"))
    print("Deleted checkpoints")

# %%
streamingQuery = generate_and_append_data_stream()

# %%
# Run the following cell multiple times
spark.sql("SELECT count(*) FROM loans_delta").show()

# %%
stop_all_streams()

# %%
cols = ['loan_id', 'funded_amnt', 'paid_amnt', 'addr_state', 'closed']

items = [
  (1111111, 1000, 1000.0, 'TX', True),
  (2222222, 2000, 0.0, 'CA', False)
]

loanUpdates = (spark
                .createDataFrame(items, cols)
                .withColumn("funded_amnt", F.col("funded_amnt").cast("int")))

# %%
# Uncomment the line below and it will error
# loanUpdates.write.format("delta").mode("append").save(deltaPath)

# %%
(loanUpdates.write.format("delta").mode("append")
    .option("mergeSchema", "true")
    .save(deltaPath))

# %%
spark.read.format("delta").load(deltaPath).where("loan_id = 1111111").show()

# %%
spark.read.format("delta").load(deltaPath).show()

# %%
spark.read.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta")
print("Defined view 'loans_delta'")

# %%
spark.sql("""
SELECT
    addr_state,
    count(addr_state)
FROM loans_delta
WHERE addr_state IN ('OR', 'WA', 'CA', 'TX', 'NY')
GROUP BY addr_state
""").show()

# %%
# Let's fix the data.
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, deltaPath)
deltaTable.update("addr_state = 'OR'",  {"addr_state": "'WA'"})

# %%
spark.sql("""
SELECT
  addr_state,
  count(1)
FROM loans_delta
WHERE addr_state IN ('OR', 'WA', 'CA', 'TX', 'NY')
GROUP BY addr_state
""").show()

# %%
spark.sql("SELECT COUNT(*) FROM loans_delta WHERE funded_amnt = paid_amnt").show()

# %%
deltaTable = DeltaTable.forPath(spark, deltaPath)
deltaTable.delete("funded_amnt = paid_amnt")

# %%
spark.sql("SELECT COUNT(*) FROM loans_delta WHERE funded_amnt = paid_amnt").show()

# %%
spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# %%
cols = ['loan_id', 'funded_amnt', 'paid_amnt', 'addr_state', 'closed']

items = [
  (11, 1000, 1000.0, 'NY', True),   # loan paid off
  (12, 1000, 0.0, 'NY', False)      # new loan
]

loanUpdates = spark.createDataFrame(items, cols)

# %%
deltaTable = DeltaTable.forPath(spark, deltaPath)

(deltaTable
    .alias("t")
    .merge(loanUpdates.alias("s"), "t.loan_id = s.loan_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# %%
spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# %%
cols = ['loan_id', 'funded_amnt', 'paid_amnt', 'addr_state', 'closed']

items = [
  (11, 1000, 0.0, 'NY', False),
  (-100, 1000, 10.0, 'NY', False)
]

historicalUpdates = spark.createDataFrame(items, cols)

# %%
deltaTable = DeltaTable.forPath(spark, deltaPath)

(deltaTable
  .alias("t")
  .merge(historicalUpdates.alias("s"), "t.loan_id = s.loan_id")
  .whenNotMatchedInsertAll()
  .execute())

# %%
spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# %%
deltaTable = DeltaTable.forPath(spark, deltaPath)
deltaTable.history().show()

# %%
(deltaTable
    .history(4)
    .select("version", "timestamp", "operation", "operationParameters")
    .show(truncate=False))

# %%
previousVersion = deltaTable.history(1).select("version").first()[0] - 3
previousVersion

# %%
(spark.read.format("delta")
    .option("versionAsOf", previousVersion)
    .load(deltaPath)
    .createOrReplaceTempView("loans_delta_pre_delete"))

spark.sql("SELECT COUNT(*) FROM loans_delta_pre_delete WHERE funded_amnt = paid_amnt").show()
