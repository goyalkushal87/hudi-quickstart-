"""
Phase 6 -- Structured Streaming with Hudi: sink AND source.

Part A: use Spark's built-in `rate` source (fake streaming data, no external
system needed -- zero cost) to continuously write into a Hudi table. Each
micro-batch becomes its own Hudi commit.

Part B: read that SAME table back as a streaming SOURCE, proving it picks up
new commits incrementally (built on the same incremental-query mechanics we
just learned, but with Spark managing the checkpoint automatically).

Run:
    docker exec -it spark-hudi /opt/spark/bin/spark-submit \
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
      --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
      /home/hudi/notebooks/phase6_hudi_streaming.py 2>&1 | grep -E "\[hudi\]|Batch:"
"""
import os
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

TABLE_NAME = "orders_streaming_demo"
TABLE_PATH = "/home/hudi/notebooks/warehouse/orders_streaming_demo"
CHECKPOINT_WRITE = "/home/hudi/notebooks/warehouse/_checkpoints/orders_streaming_write"
CHECKPOINT_READ = "/home/hudi/notebooks/warehouse/_checkpoints/orders_streaming_read"

for p in (TABLE_PATH, CHECKPOINT_WRITE, CHECKPOINT_READ):
    if os.path.exists(p):
        shutil.rmtree(p)

spark = SparkSession.builder.appName("hudi-phase6-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "order_date",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.index.type": "BLOOM",
}

# =====================================================================
# Part A: rate source -> Hudi table (streaming SINK)
# =====================================================================
print("[hudi] Part A: starting streaming write (rate source -> Hudi), 5-second micro-batches, running ~20s")
rate_df = spark.readStream.format("rate").option("rowsPerSecond", 2).load()
orders_df = rate_df.select(
    col("value").cast("string").alias("order_id"),
    (col("value") % 5).cast("string").alias("order_status_code"),
    (col("value") * 10.0).alias("amount"),
    col("timestamp").cast("string").alias("updated_at"),
).selectExpr(
    "concat('s', order_id) as order_id",
    "'created' as order_status",
    "amount",
    "updated_at",
    "'2026-09-01' as order_date",
)

write_query = orders_df.writeStream \
    .format("hudi") \
    .options(**hudi_options) \
    .option("checkpointLocation", CHECKPOINT_WRITE) \
    .trigger(processingTime="5 seconds") \
    .outputMode("append") \
    .start(TABLE_PATH)

time.sleep(20)
write_query.stop()
print("[hudi] Part A: stopped the streaming write after ~20s")

written_df = spark.read.format("hudi").load(TABLE_PATH)
n_rows = written_df.count()
n_commits = written_df.select("_hoodie_commit_time").distinct().count()
print(f"[hudi] Part A result: {n_rows} row(s) written across {n_commits} separate commit(s) (one per micro-batch)")

# =====================================================================
# Part B: Hudi table -> streaming SOURCE, write to console
# =====================================================================
print("[hudi] Part B: starting streaming read FROM the Hudi table (readStream), running ~10s")
read_stream_df = spark.readStream.format("hudi").load(TABLE_PATH)

read_query = read_stream_df.select("order_id", "amount", "_hoodie_commit_time") \
    .writeStream \
    .format("console") \
    .option("checkpointLocation", CHECKPOINT_READ) \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

time.sleep(10)
read_query.stop()
print("[hudi] Part B: stopped the streaming read after ~10s -- check the console output above for streamed batches")

print("[hudi] Phase 6 complete.")
