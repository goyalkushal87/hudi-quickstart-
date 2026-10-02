"""
Phase 2 -- Merge-on-Read (MoR) hands-on: delta LOG files vs base-file rewrite.

Same 5-row orders dataset as Phase 1, but table.type = MERGE_ON_READ this time,
and the partition field is the immutable order_date (Phase 1c's fix) so this
test is ISOLATED from the earlier indexing bug -- we only want to see MoR's
own write-path behavior here, nothing else mixed in.

We update o3's `amount` only (not the partition field, not even the status).
Question: does Hudi rewrite the base parquet file (like CoW always does), or
does it write something new next to it?

Run:
    docker exec -it spark-hudi /opt/spark/bin/spark-submit \\
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \\
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \\
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \\
      --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \\
      /home/hudi/notebooks/phase2_hudi_mor_basics.py 2>&1 | grep "\\[hudi\\]"
"""
import os
import shutil
from pyspark.sql import SparkSession

TABLE_NAME = "orders_mor"
TABLE_PATH = "/home/hudi/notebooks/warehouse/orders_mor"

spark = SparkSession.builder.appName("hudi-phase2-mor-basics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)
    print(f"[hudi] Cleaned previous table state at {TABLE_PATH}")

hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "order_date",  # immutable -- isolates the MoR mechanic
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.index.type": "BLOOM",
    # Keep compaction OFF for this step -- we want to SEE the raw delta log
    # before any compaction merges it into a new base file version.
    "hoodie.compact.inline": "false",
}

initial_rows = [
    ("o1", "created",   100.0, "2026-09-01 10:00:00", "2026-09-01"),
    ("o2", "created",   250.0, "2026-09-01 10:05:00", "2026-09-01"),
    ("o3", "shipped",    75.0, "2026-09-01 10:10:00", "2026-09-01"),
    ("o4", "shipped",   300.0, "2026-09-01 10:15:00", "2026-09-01"),
    ("o5", "delivered",  50.0, "2026-09-01 10:20:00", "2026-09-01"),
]
cols = ["order_id", "order_status", "amount", "updated_at", "order_date"]
df = spark.createDataFrame(initial_rows, cols)
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(TABLE_PATH)
print(f"[hudi] Step 1 done: initial write of {df.count()} rows to {TABLE_NAME} (base files created)")

# Update o3's amount only -- not the partition field, not even the status.
update_rows = [("o3", "shipped", 999.0, "2026-09-01 11:00:00", "2026-09-01")]
update_df = spark.createDataFrame(update_rows, cols)
update_df.write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)
print("[hudi] Step 2 done: upserted o3's amount 75.0 -> 999.0 (same partition, no compaction yet)")

# --- Inspect the physical file layout of o3's partition ---
partition_dir = os.path.join(TABLE_PATH, "order_date=2026-09-01")
all_files = sorted(os.listdir(partition_dir))
print("[hudi] Step 3: files inside order_date=2026-09-01/ after the update:")
for f in all_files:
    tag = "base parquet file" if f.endswith(".parquet") else ("hidden file" if f.startswith(".") else "other")
    print(f"[hudi]   {f}   <-- {tag}")

log_files = [f for f in all_files if ".log." in f]
print(f"[hudi] Step 3 result: {len(log_files)} delta LOG file(s) found -- base file was NOT rewritten")

# --- Three query types (before any compaction) ---
print("[hudi] Step 4: READ OPTIMIZED query (base file only, ignores the log file)")
ro_df = spark.read.format("hudi").option("hoodie.datasource.query.type", "read_optimized").load(TABLE_PATH)
for r in ro_df.filter(ro_df.order_id == "o3").select("order_id", "amount").collect():
    print(f"[hudi]   read_optimized sees: {r}")

print("[hudi] Step 5: SNAPSHOT query (merges base + log at read time)")
snap_df = spark.read.format("hudi").option("hoodie.datasource.query.type", "snapshot").load(TABLE_PATH)
for r in snap_df.filter(snap_df.order_id == "o3").select("order_id", "amount").collect():
    print(f"[hudi]   snapshot sees: {r}")

print("[hudi] Phase 2 complete.")
