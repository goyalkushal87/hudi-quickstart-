"""
Phase 1b -- Fix the partition-value-change duplicate using a GLOBAL index.

Same partition field (order_status, which is MUTABLE -- it changes when an
order's status changes) as Phase 1, but this time we tell Hudi's index to
look across ALL partitions for a matching record key, not just the incoming
record's own partition.

Run:
    docker exec -it spark-hudi spark-submit \\
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \\
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \\
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \\
      --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \\
      /home/hudi/notebooks/phase1b_global_index_fix.py 2>&1 | grep "\\[hudi\\]"
"""
import os
import shutil
from pyspark.sql import SparkSession

TABLE_NAME = "orders_cow_global"
TABLE_PATH = "/home/hudi/notebooks/warehouse/orders_cow_global"

spark = SparkSession.builder.appName("hudi-phase1b-global-index").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)
    print(f"[hudi] Cleaned previous table state at {TABLE_PATH}")

hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "order_status",  # still mutable
    "hoodie.datasource.write.hive_style_partitioning": "true",
    # --- the fix: look across ALL partitions for a matching key ---
    "hoodie.index.type": "GLOBAL_BLOOM",
    # --- and if found in a different partition than the incoming record's,
    # actually MOVE it there (delete old copy, write new one) instead of
    # leaving a stale copy behind in the old partition ---
    "hoodie.bloom.index.update.partition.path": "true",
}

initial_rows = [
    ("o1", "created",   100.0, "2026-09-01 10:00:00"),
    ("o2", "created",   250.0, "2026-09-01 10:05:00"),
    ("o3", "shipped",    75.0, "2026-09-01 10:10:00"),
    ("o4", "shipped",   300.0, "2026-09-01 10:15:00"),
    ("o5", "delivered",  50.0, "2026-09-01 10:20:00"),
]
df = spark.createDataFrame(initial_rows, ["order_id", "order_status", "amount", "updated_at"])
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(TABLE_PATH)
print(f"[hudi] Step 1 done: initial write of {df.count()} rows to {TABLE_NAME}")

update_rows = [("o3", "delivered", 75.0, "2026-09-01 11:00:00")]  # partition-changing update
update_df = spark.createDataFrame(update_rows, ["order_id", "order_status", "amount", "updated_at"])
update_df.write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)
print("[hudi] Step 2 done: upserted o3 (shipped -> delivered), a partition-path-changing update")

after_df = spark.read.format("hudi").load(TABLE_PATH)
total = after_df.count()
print(f"[hudi] Step 2 verify: table has {total} rows (expected 5 -- no duplicate)")

o3_rows = after_df.filter(after_df.order_id == "o3").select("order_id", "order_status", "amount")
print(f"[hudi] Step 2 verify: o3 appears in {o3_rows.count()} row(s) across the table:")
for r in o3_rows.collect():
    print(f"[hudi]   {r}")

print("[hudi] Confirming the stale shipped/ copy is really gone (not just hidden by a merge):")
shipped_partition = os.path.join(TABLE_PATH, "order_status=shipped")
if os.path.exists(shipped_partition):
    files = [f for f in os.listdir(shipped_partition) if f.endswith(".parquet")]
    print(f"[hudi]   order_status=shipped/ has {len(files)} live parquet file(s)")
    check_df = spark.read.parquet(*[os.path.join(shipped_partition, f) for f in files]) if files else None
    if check_df is not None:
        remaining_o3 = check_df.filter(check_df.order_id == "o3").count()
        print(f"[hudi]   o3 rows physically still present in shipped/ base files: {remaining_o3}")

print("[hudi] Phase 1b complete.")
