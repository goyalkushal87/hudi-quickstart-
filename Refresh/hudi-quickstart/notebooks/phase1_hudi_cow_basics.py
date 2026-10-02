"""
Phase 1 -- Apache Hudi basics: Copy-on-Write (CoW) table lifecycle.

Run as an EMR-style Spark Step:
    docker exec -it spark-hudi spark-submit \\
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \\
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \\
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \\
      --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \\
      /home/hudi/notebooks/phase1_hudi_cow_basics.py 2>&1 | grep "\\[hudi\\]"

What this proves, step by step:
  1. A brand-new CoW table has exactly one commit after the first write.
  2. An UPSERT that touches an existing record forces Hudi to rewrite the
     whole base file it lives in (Copy-on-Write -- same behavior we saw
     with Iceberg's CDC MERGE in the capstone project).
  3. The .hoodie/ metadata folder is Hudi's equivalent of Iceberg's
     snapshot/manifest chain -- this script lists it so the *structure*
     of the timeline is visible before we study it conceptually.
"""
import os
import shutil
from pyspark.sql import SparkSession

TABLE_NAME = "orders_cow"
TABLE_PATH = "/home/hudi/notebooks/warehouse/orders_cow"

spark = SparkSession.builder \
    .appName("hudi-phase1-cow-basics") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Clean slate so this script is safely re-runnable.
if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)
    print(f"[hudi] Cleaned previous table state at {TABLE_PATH}")

hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "order_status",
    "hoodie.datasource.write.hive_style_partitioning": "true",
}

# --- Step 1: first write (bulk insert) ---
initial_rows = [
    ("o1", "created",   100.0, "2026-09-01 10:00:00"),
    ("o2", "created",   250.0, "2026-09-01 10:05:00"),
    ("o3", "shipped",    75.0, "2026-09-01 10:10:00"),
    ("o4", "shipped",   300.0, "2026-09-01 10:15:00"),
    ("o5", "delivered",  50.0, "2026-09-01 10:20:00"),
]
df = spark.createDataFrame(initial_rows, ["order_id", "order_status", "amount", "updated_at"])

df.write.format("hudi") \
    .options(**hudi_options) \
    .mode("overwrite") \
    .save(TABLE_PATH)

print(f"[hudi] Step 1 done: initial write of {df.count()} rows to {TABLE_NAME}")

read_df = spark.read.format("hudi").load(TABLE_PATH)
print(f"[hudi] Step 1 verify: table now has {read_df.count()} rows")

# --- Step 2: UPSERT touching ONE existing record (o3: shipped -> delivered) ---
update_rows = [
    ("o3", "delivered", 75.0, "2026-09-01 11:00:00"),  # status change, same key
]
update_df = spark.createDataFrame(update_rows, ["order_id", "order_status", "amount", "updated_at"])

update_df.write.format("hudi") \
    .options(**hudi_options) \
    .mode("append") \
    .save(TABLE_PATH)

print("[hudi] Step 2 done: upserted o3 (shipped -> delivered)")

after_df = spark.read.format("hudi").load(TABLE_PATH)
print(f"[hudi] Step 2 verify: table still has {after_df.count()} rows (upsert, not insert)")
o3_row = after_df.filter(after_df.order_id == "o3").select("order_id", "order_status", "amount").collect()
print(f"[hudi] Step 2 verify: o3's current state -> {o3_row}")

# --- Step 3: inspect the .hoodie metadata folder (Hudi's timeline) ---
hoodie_meta_path = os.path.join(TABLE_PATH, ".hoodie")
print(f"[hudi] Step 3: contents of {hoodie_meta_path}")
for entry in sorted(os.listdir(hoodie_meta_path)):
    full = os.path.join(hoodie_meta_path, entry)
    if os.path.isfile(full):
        print(f"[hudi]   FILE  {entry}  ({os.path.getsize(full)} bytes)")

timeline_dir = os.path.join(hoodie_meta_path, "timeline")
if os.path.exists(timeline_dir):
    print(f"[hudi] Step 3: contents of {timeline_dir}")
    for entry in sorted(os.listdir(timeline_dir)):
        print(f"[hudi]   INSTANT  {entry}")

print("[hudi] Phase 1 complete.")
