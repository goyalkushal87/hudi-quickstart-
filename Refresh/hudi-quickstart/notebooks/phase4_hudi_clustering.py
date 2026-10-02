"""
Phase 4 -- Clustering: bin-packing small files + sorting, on a CoW table.

We deliberately create the "many small files" problem (like the Iceberg
638-file incident, but on purpose and small-scale here) by disabling Hudi's
built-in small-file auto-packing (hoodie.parquet.small.file.limit=0) and
writing 5 separate single-row batches into the SAME partition. Each write
then creates its own brand-new file group instead of being packed into an
existing small file.

Then we run Clustering to (a) bin-pack these into fewer files and (b) sort
the records by `amount`, and PROVE the sort really happened by reading the
resulting file directly (raw parquet, preserving on-disk row order).

Run:
    docker exec -it spark-hudi /opt/spark/bin/spark-submit \\
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \\
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \\
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \\
      --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \\
      /home/hudi/notebooks/phase4_hudi_clustering.py 2>&1 | grep "\\[hudi\\]"
"""
import os
import shutil
from pyspark.sql import SparkSession

TABLE_NAME = "orders_clustering_demo"
TABLE_PATH = "/home/hudi/notebooks/warehouse/orders_clustering_demo"
PARTITION_DIR = os.path.join(TABLE_PATH, "order_date=2026-09-01")

spark = SparkSession.builder.appName("hudi-phase4-clustering").getOrCreate()
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
    # Disable Hudi's own small-file auto-packing so every write below
    # creates its OWN new file group -- this is what artificially recreates
    # the "many small files" problem for us to then fix with Clustering.
    "hoodie.parquet.small.file.limit": "0",
}

# 5 separate single-row batches, shuffled amounts, all into the SAME partition.
rows_in_order = [
    ("c1", "created", 500.0, "2026-09-01 09:00:00", "2026-09-01"),
    ("c2", "created", 100.0, "2026-09-01 09:01:00", "2026-09-01"),
    ("c3", "created", 300.0, "2026-09-01 09:02:00", "2026-09-01"),
    ("c4", "created", 200.0, "2026-09-01 09:03:00", "2026-09-01"),
    ("c5", "created", 400.0, "2026-09-01 09:04:00", "2026-09-01"),
]
for row in rows_in_order:
    row_df = spark.createDataFrame([row], cols)
    row_df.write.format("hudi").options(**hudi_options).mode("append").save(TABLE_PATH)

before_files = [f for f in sorted(os.listdir(PARTITION_DIR)) if f.endswith(".parquet")]
print(f"[hudi] Step 1: wrote 5 separate small batches -- {len(before_files)} base parquet file(s) before clustering")
for f in before_files:
    print(f"[hudi]   {f}")

total_before = spark.read.format("hudi").load(TABLE_PATH).count()
print(f"[hudi] Step 1: total row count = {total_before}")

# --- Step 2: run clustering -- bin-pack + sort by amount ---
print("[hudi] Step 2: running CALL run_clustering(order => 'amount', order_strategy => 'linear', ...)")
result = spark.sql(
    f"CALL run_clustering(path => '{TABLE_PATH}', order => 'amount', order_strategy => 'linear')"
)
for r in result.collect():
    print(f"[hudi]   clustering result: {r}")

# --- Step 3: inspect the partition folder again ---
after_files = [f for f in sorted(os.listdir(PARTITION_DIR)) if f.endswith(".parquet")]
print(f"[hudi] Step 3: {len(after_files)} base parquet file(s) after clustering")
for f in after_files:
    print(f"[hudi]   {f}")

total_after = spark.read.format("hudi").load(TABLE_PATH).count()
print(f"[hudi] Step 3: total row count = {total_after} (should still be 5 -- clustering doesn't change data, only layout)")

# --- Step 4: prove the SORT actually happened -- read the raw file(s), preserving on-disk order ---
# We deliberately bypass the Hudi reader here and read the parquet file directly,
# the same way we proved file-group facts in earlier phases.
newest_file = sorted(after_files)[-1] if after_files else None
if newest_file:
    raw_df = spark.read.parquet(os.path.join(PARTITION_DIR, newest_file))
    print(f"[hudi] Step 4: raw on-disk row order inside {newest_file}:")
    for r in raw_df.select("order_id", "amount").collect():
        print(f"[hudi]   {r}")

print("[hudi] Phase 4 complete.")
