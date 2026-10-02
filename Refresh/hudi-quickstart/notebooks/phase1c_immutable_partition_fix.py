"""
Phase 1c -- The architecturally better fix: partition by an IMMUTABLE field.

Instead of paying the extra cost of a GLOBAL index on every write (Phase 1b),
this script removes the root cause entirely: partition on order_date, a value
derived once at order-creation time that never changes when order_status
changes later. The default (non-global) Bloom Index works correctly here
because an updated record's partition path never moves.

Run:
    docker exec -it spark-hudi spark-submit \\
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \\
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \\
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \\
      --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \\
      /home/hudi/notebooks/phase1c_immutable_partition_fix.py 2>&1 | grep "\\[hudi\\]"
"""
import os
import shutil
from pyspark.sql import SparkSession

TABLE_NAME = "orders_cow_immutable_partition"
TABLE_PATH = "/home/hudi/notebooks/warehouse/orders_cow_immutable_partition"

spark = SparkSession.builder.appName("hudi-phase1c-immutable-partition").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)
    print(f"[hudi] Cleaned previous table state at {TABLE_PATH}")

hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    # order_date is set ONCE when the order is created and never touched
    # again by any later update -- unlike order_status, it cannot move a
    # record between partitions.
    "hoodie.datasource.write.partitionpath.field": "order_date",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    # Default index (BLOOM, non-global) -- no extra global-lookup cost.
    "hoodie.index.type": "BLOOM",
}

# order_date is derived from order_created_at once, at insert time.
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
print(f"[hudi] Step 1 done: initial write of {df.count()} rows to {TABLE_NAME}")

# Same status-changing update as before -- order_date is carried forward
# UNCHANGED (it's not derived from updated_at, it's the original creation
# date), so the partition path for o3 stays order_date=2026-09-01.
update_rows = [("o3", "delivered", 75.0, "2026-09-01 11:00:00", "2026-09-01")]
update_df = spark.createDataFrame(update_rows, cols)
update_df.write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)
print("[hudi] Step 2 done: upserted o3 (shipped -> delivered); order_date unchanged")

after_df = spark.read.format("hudi").load(TABLE_PATH)
total = after_df.count()
print(f"[hudi] Step 2 verify: table has {total} rows (expected 5 -- no duplicate, no global index needed)")

o3_rows = after_df.filter(after_df.order_id == "o3").select("order_id", "order_status", "amount")
print(f"[hudi] Step 2 verify: o3 appears in {o3_rows.count()} row(s):")
for r in o3_rows.collect():
    print(f"[hudi]   {r}")

print("[hudi] Phase 1c complete.")
