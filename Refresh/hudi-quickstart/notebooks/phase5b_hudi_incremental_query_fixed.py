"""
Phase 5b -- Incremental Query, FIXED: isolate each record into its OWN file
group (small.file.limit=0) so a change to i3 does NOT drag i1/i2 along as
false positives. This is the same fix pattern as Phase 4's clustering demo.

Timeline:
  T0: insert i1(100), i2(200), i3(300)  -- each as its OWN file (small.file.limit=0)
  T1: upsert i3 -> 350                  (rewrites ONLY i3's file now)
  T2: upsert i3 -> 999, insert i4(400)  (rewrites i3's file again; i4 gets its own new file)

Run:
    docker exec -it spark-hudi /opt/spark/bin/spark-submit \
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
      --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
      /home/hudi/notebooks/phase5b_hudi_incremental_query_fixed.py 2>&1 | grep "\[hudi\]"
"""
import os
import shutil
from pyspark.sql import SparkSession

TABLE_NAME = "orders_incremental_demo_fixed"
TABLE_PATH = "/home/hudi/notebooks/warehouse/orders_incremental_demo_fixed"
PARTITION_DIR = os.path.join(TABLE_PATH, "order_date=2026-09-01")

spark = SparkSession.builder.appName("hudi-phase5b-incremental-fixed").getOrCreate()
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
    # THE FIX: each record's file group stays isolated -- an update to i3
    # never gets packed together with i1/i2's untouched file.
    "hoodie.parquet.small.file.limit": "0",
}

# --- T0: i1, i2, i3 as THREE SEPARATE writes -> three separate file groups ---
for row in [
    ("i1", "created", 100.0, "2026-09-01 09:00:00", "2026-09-01"),
    ("i2", "created", 200.0, "2026-09-01 09:01:00", "2026-09-01"),
    ("i3", "created", 300.0, "2026-09-01 09:02:00", "2026-09-01"),
]:
    spark.createDataFrame([row], cols).write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)

T0 = spark.read.format("hudi").load(TABLE_PATH).selectExpr("max(_hoodie_commit_time) as t").collect()[0]["t"]
n_files_t0 = len([f for f in os.listdir(PARTITION_DIR) if f.endswith(".parquet")])
print(f"[hudi] T0: i1,i2,i3 written as 3 separate commits, last one at {T0} -- {n_files_t0} file group(s) now")

# --- T1: update i3 only ---
spark.createDataFrame([("i3", "created", 350.0, "2026-09-01 09:10:00", "2026-09-01")], cols) \
    .write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)
T1 = spark.read.format("hudi").load(TABLE_PATH).selectExpr("max(_hoodie_commit_time) as t").collect()[0]["t"]
print(f"[hudi] T1 (i3 -> 350) committed at {T1}")

# --- T2: update i3 again + insert new i4 ---
spark.createDataFrame([("i3", "created", 999.0, "2026-09-01 09:20:00", "2026-09-01")], cols) \
    .write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)
spark.createDataFrame([("i4", "created", 400.0, "2026-09-01 09:21:00", "2026-09-01")], cols) \
    .write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)
T2 = spark.read.format("hudi").load(TABLE_PATH).selectExpr("max(_hoodie_commit_time) as t").collect()[0]["t"]
n_files_t2 = len([f for f in os.listdir(PARTITION_DIR) if f.endswith(".parquet")])
print(f"[hudi] T2 (i3 -> 999, insert i4) committed at {T2} -- {n_files_t2} file group(s) now")

# =====================================================================
# A) Incremental query from T0 (exclusive) to latest
# =====================================================================
incr_full = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", T0) \
    .load(TABLE_PATH)
rows_full = incr_full.select("order_id", "amount").orderBy("order_id").collect()
print(f"[hudi] Step A: incremental begin={T0} (exclusive) -> {len(rows_full)} record(s): {rows_full}")
print("[hudi] Step A: expect ONLY i3=999.0 and i4=400.0 -- i1/i2 must be ABSENT this time")

# =====================================================================
# B) Narrow window: begin=T0, end=T1
# =====================================================================
incr_narrow = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", T0) \
    .option("hoodie.datasource.read.end.instanttime", T1) \
    .load(TABLE_PATH)
rows_narrow = incr_narrow.select("order_id", "amount").orderBy("order_id").collect()
print(f"[hudi] Step B: incremental begin={T0}, end={T1} -> {len(rows_narrow)} record(s): {rows_narrow}")
print("[hudi] Step B: expect ONLY i3=350.0 -- clean, unambiguous now")

print("[hudi] Phase 5b complete.")
