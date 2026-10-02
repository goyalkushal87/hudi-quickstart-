"""
Phase 2b -- Trigger compaction on the existing orders_mor table, then prove
that time travel across a compaction boundary still works.

This does NOT recreate orders_mor -- it opens the table Phase 2 already wrote
(1 base file + 1 delta log, holding o3's update 75.0 -> 999.0 unmerged).

Question we're answering hands-on: once compaction runs and merges everything
into a fresh base file, can we still read the table as it looked BEFORE the
update (amount=75.0), or is that old state gone for good?

Run:
    docker exec -it spark-hudi /opt/spark/bin/spark-submit \\
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \\
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \\
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \\
      --conf 'spark.kryo.registrator=org.apache.spark.spark.HoodieSparkKryoRegistrar' \\
      /home/hudi/notebooks/phase2b_hudi_compaction_and_time_travel.py 2>&1 | grep "\\[hudi\\]"
"""
import os
from pyspark.sql import SparkSession

TABLE_PATH = "/home/hudi/notebooks/warehouse/orders_mor"

spark = SparkSession.builder.appName("hudi-phase2b-compaction-time-travel").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if not os.path.exists(TABLE_PATH):
    raise SystemExit(f"[hudi] {TABLE_PATH} not found -- run phase2_hudi_mor_basics.py first.")

# --- Step 1: find the two instants that already exist (from Phase 2) ---
before_df = spark.read.format("hudi").load(TABLE_PATH)
instants = [r["_hoodie_commit_time"] for r in before_df.select("_hoodie_commit_time").distinct().collect()]
instants.sort()
T0 = instants[0]   # initial write (o3 amount = 75.0)
T1 = instants[-1]  # delta commit that updated o3 (amount = 999.0, sitting in the log file)
print(f"[hudi] Step 1: found instants on the timeline -- T0={T0} (initial write), T1={T1} (o3's update)")

# --- Step 2: run compaction (schedule + execute in one call) ---
print("[hudi] Step 2: running CALL run_compaction(op => 'run', ...)")
result = spark.sql(f"CALL run_compaction(op => 'run', path => '{TABLE_PATH}')")
for r in result.collect():
    print(f"[hudi]   compaction result: {r}")

# --- Step 3: inspect the partition folder again ---
partition_dir = os.path.join(TABLE_PATH, "order_date=2026-09-01")
all_files = sorted(os.listdir(partition_dir))
print("[hudi] Step 3: files inside order_date=2026-09-01/ after compaction:")
for f in all_files:
    if f.endswith(".crc"):
        continue  # checksums -- not real data/log files, skip for clarity
    tag = "base parquet file" if f.endswith(".parquet") else ("delta log file" if ".log." in f else "other")
    print(f"[hudi]   {f}   <-- {tag}")

# --- Step 4: time-travel back to T0 (BEFORE o3's update, and before compaction) ---
print(f"[hudi] Step 4: time-travel query as.of.instant={T0} (should show o3's ORIGINAL amount)")
t0_df = spark.read.format("hudi").option("as.of.instant", T0).load(TABLE_PATH)
for r in t0_df.filter(t0_df.order_id == "o3").select("order_id", "amount").collect():
    print(f"[hudi]   as.of T0 sees: {r}")

# --- Step 5: time-travel to T1 (AFTER the update, BEFORE compaction) ---
print(f"[hudi] Step 5: time-travel query as.of.instant={T1} (should show o3's UPDATED amount, pre-compaction)")
t1_df = spark.read.format("hudi").option("as.of.instant", T1).load(TABLE_PATH)
for r in t1_df.filter(t1_df.order_id == "o3").select("order_id", "amount").collect():
    print(f"[hudi]   as.of T1 sees: {r}")

# --- Step 6: latest READ OPTIMIZED query, now AFTER compaction ---
print("[hudi] Step 6: READ OPTIMIZED query on the LATEST state, now that compaction has run")
ro_df = spark.read.format("hudi").option("hoodie.datasource.query.type", "read_optimized").load(TABLE_PATH)
for r in ro_df.filter(ro_df.order_id == "o3").select("order_id", "amount").collect():
    print(f"[hudi]   read_optimized (post-compaction) sees: {r}")

print("[hudi] Phase 2b complete.")
