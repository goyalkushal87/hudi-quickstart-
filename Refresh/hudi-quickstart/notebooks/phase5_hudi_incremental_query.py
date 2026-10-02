"""
Phase 5 -- Incremental Query: read only what CHANGED between two instants.

Timeline we build:
  T0: insert i1(100), i2(200), i3(300)
  T1: upsert i3 -> 350                      (first change to i3)
  T2: upsert i3 -> 999, insert i4(400)      (second change to i3, in the SAME window)

Two things to prove:
  A) incremental query from T0 (exclusive) to latest should show i3 and i4 ONLY
     -- i1 and i2 were never touched after T0, so they must NOT appear.
  B) i3 changed TWICE (T1: 350, T2: 999) inside that window -- incremental
     query should show only the FINAL value (999), not both.

Run:
    docker exec -it spark-hudi /opt/spark/bin/spark-submit \
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
      --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
      /home/hudi/notebooks/phase5_hudi_incremental_query.py 2>&1 | grep "\[hudi\]"
"""
import os
import shutil
from pyspark.sql import SparkSession

TABLE_NAME = "orders_incremental_demo"
TABLE_PATH = "/home/hudi/notebooks/warehouse/orders_incremental_demo"

spark = SparkSession.builder.appName("hudi-phase5-incremental-query").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)

cols = ["order_id", "order_status", "amount", "updated_at", "order_date"]
hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "order_date",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.index.type": "BLOOM",
}

# --- T0: initial insert of i1, i2, i3 ---
t0_rows = [
    ("i1", "created", 100.0, "2026-09-01 09:00:00", "2026-09-01"),
    ("i2", "created", 200.0, "2026-09-01 09:01:00", "2026-09-01"),
    ("i3", "created", 300.0, "2026-09-01 09:02:00", "2026-09-01"),
]
spark.createDataFrame(t0_rows, cols).write.format("hudi").options(**hudi_options).mode("overwrite").save(TABLE_PATH)
T0 = spark.read.format("hudi").load(TABLE_PATH).selectExpr("max(_hoodie_commit_time) as t").collect()[0]["t"]
print(f"[hudi] T0 (initial insert i1,i2,i3) committed at {T0}")

# --- T1: first change to i3 ---
t1_rows = [("i3", "created", 350.0, "2026-09-01 09:10:00", "2026-09-01")]
spark.createDataFrame(t1_rows, cols).write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)
T1 = spark.read.format("hudi").load(TABLE_PATH).selectExpr("max(_hoodie_commit_time) as t").collect()[0]["t"]
print(f"[hudi] T1 (i3 -> 350) committed at {T1}")

# --- T2: second change to i3 (SAME window), plus a brand new i4 ---
t2_rows = [
    ("i3", "created", 999.0, "2026-09-01 09:20:00", "2026-09-01"),
    ("i4", "created", 400.0, "2026-09-01 09:21:00", "2026-09-01"),
]
spark.createDataFrame(t2_rows, cols).write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)
T2 = spark.read.format("hudi").load(TABLE_PATH).selectExpr("max(_hoodie_commit_time) as t").collect()[0]["t"]
print(f"[hudi] T2 (i3 -> 999, insert i4) committed at {T2}")

# =====================================================================
# A) Incremental query from T0 (exclusive) to latest
# =====================================================================
incr_full = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", T0) \
    .load(TABLE_PATH)

rows_full = incr_full.select("order_id", "amount").orderBy("order_id").collect()
print(f"[hudi] Step A: incremental query begin={T0} (exclusive) -> {len(rows_full)} record(s) changed:")
for r in rows_full:
    print(f"[hudi]   {r}")
print("[hudi] Step A: i1 and i2 should be ABSENT (never touched after T0)")
print("[hudi] Step A: i3 should show ONLY 999 (final value), not 350 (the intermediate T1 value)")

# =====================================================================
# B) Narrow window: begin=T0, end=T1 -- should catch ONLY the T1 change (350)
# =====================================================================
incr_narrow = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", T0) \
    .option("hoodie.datasource.read.end.instanttime", T1) \
    .load(TABLE_PATH)

rows_narrow = incr_narrow.select("order_id", "amount").orderBy("order_id").collect()
print(f"[hudi] Step B: incremental query begin={T0}, end={T1} -> {len(rows_narrow)} record(s) changed:")
for r in rows_narrow:
    print(f"[hudi]   {r}")
print("[hudi] Step B: should show ONLY i3 = 350.0 -- T2's change (999.0) and i4 are outside this window")

print("[hudi] Phase 5 complete.")
