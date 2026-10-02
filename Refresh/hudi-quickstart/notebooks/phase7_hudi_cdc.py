"""
Phase 7 -- Hudi's dedicated CDC (Change Data Capture) feature.

Unlike a plain incremental query (which just says "these files changed, go
read the current state yourself"), CDC records the EXACT change event at
write time: what operation happened (insert/update/delete), and (in
DATA_BEFORE_AFTER mode) the full before-image AND after-image of the row.

Timeline we build on table `orders_cdc_demo`:
  T0: insert i1(100), i2(200)
  T1: update i1 -> 150            (upsert)
  T2: delete i2
  T3: insert i3(300)

Then we read the CDC change stream from T0 (exclusive) to latest, and print
the op / before / after columns directly -- no guessing needed.

Run:
    docker exec -it spark-hudi /opt/spark/bin/spark-submit \\
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \\
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \\
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \\
      --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \\
      /home/hudi/notebooks/phase7_hudi_cdc.py 2>&1 | grep "\\[hudi\\]"
"""
import os
import shutil
from pyspark.sql import SparkSession

TABLE_NAME = "orders_cdc_demo"
TABLE_PATH = "/home/hudi/notebooks/warehouse/orders_cdc_demo"

spark = SparkSession.builder.appName("hudi-phase7-cdc").getOrCreate()
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
    # --- the two CDC-specific configs, set at table creation ---
    "hoodie.table.cdc.enabled": "true",
    "hoodie.table.cdc.supplemental.logging.mode": "DATA_BEFORE_AFTER",
}

# --- T0: initial insert of i1, i2 ---
t0_rows = [
    ("i1", "created", 100.0, "2026-09-01 09:00:00", "2026-09-01"),
    ("i2", "created", 200.0, "2026-09-01 09:01:00", "2026-09-01"),
]
spark.createDataFrame(t0_rows, cols).write.format("hudi").options(**hudi_options).mode("overwrite").save(TABLE_PATH)
T0 = spark.read.format("hudi").load(TABLE_PATH).selectExpr("max(_hoodie_commit_time) as t").collect()[0]["t"]
print(f"[hudi] T0 (insert i1,i2) committed at {T0}")

# --- T1: update i1 -> 150 ---
t1_rows = [("i1", "created", 150.0, "2026-09-01 09:10:00", "2026-09-01")]
spark.createDataFrame(t1_rows, cols).write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)
T1 = spark.read.format("hudi").load(TABLE_PATH).selectExpr("max(_hoodie_commit_time) as t").collect()[0]["t"]
print(f"[hudi] T1 (i1 -> 150, an UPDATE) committed at {T1}")

# --- T2: delete i2 ---
spark.createDataFrame([("i2", "created", 200.0, "2026-09-01 09:01:00", "2026-09-01")], cols) \
    .write.format("hudi").options(**hudi_options) \
    .option("hoodie.datasource.write.operation", "delete") \
    .mode("append").save(TABLE_PATH)
T2 = spark.read.format("hudi").load(TABLE_PATH).selectExpr("max(_hoodie_commit_time) as t").collect()[0]["t"]
print(f"[hudi] T2 (i2 deleted) committed at {T2}")

# --- T3: insert a brand new i3 ---
t3_rows = [("i3", "created", 300.0, "2026-09-01 09:20:00", "2026-09-01")]
spark.createDataFrame(t3_rows, cols).write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)
T3 = spark.read.format("hudi").load(TABLE_PATH).selectExpr("max(_hoodie_commit_time) as t").collect()[0]["t"]
print(f"[hudi] T3 (insert i3) committed at {T3}")

# =====================================================================
# Read the CDC change stream from T0 (exclusive) to latest
# =====================================================================
cdc_df = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.query.incremental.format", "cdc") \
    .option("hoodie.datasource.read.begin.instanttime", T0) \
    .load(TABLE_PATH)

print(f"[hudi] CDC schema: {cdc_df.columns}")

rows = cdc_df.orderBy("ts_ms").collect()
print(f"[hudi] CDC change stream from T0={T0} to latest -> {len(rows)} change event(s):")
for r in rows:
    d = r.asDict()
    print(f"[hudi]   op={d.get('op')}  ts_ms={d.get('ts_ms')}  before={d.get('before')}  after={d.get('after')}")

print("[hudi] Expect 4 events: i1 insert(op=i), i1 update(op=u, before amount=100, after amount=150),")
print("[hudi] i2 delete(op=d, before amount=200, after should be null/absent), i3 insert(op=i).")
print("[hudi] Compare this to phase5: no guessing which files 'might' have changed -- every row IS a real change.")

print("[hudi] Phase 7 complete.")
