"""
Phase 3 -- Hudi write operations: insert vs upsert vs delete vs bulk_insert.

Two tables:
  A) orders_write_ops     -- proves insert doesn't dedupe, upsert doesn't fully
                              "self-heal" an existing duplicate, and delete
                              physically removes a record.
  B) orders_bulk_load     -- proves insert/bulk_insert don't dedupe WITHIN a
                              single batch either, unless hoodie.combine.before.insert
                              is explicitly turned on.

Run:
    docker exec -it spark-hudi /opt/spark/bin/spark-submit \\
      --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \\
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\
      --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \\
      --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \\
      --conf 'spark.kryo.registrator=org.apache.spark.spark.HoodieSparkKryoRegistrar' \\
      /home/hudi/notebooks/phase3_hudi_write_operations.py 2>&1 | grep "\\[hudi\\]"
"""
import os
import shutil
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hudi-phase3-write-operations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

cols = ["order_id", "order_status", "amount", "updated_at", "order_date"]

# =====================================================================
# TABLE A -- insert vs upsert vs delete
# =====================================================================
TABLE_A_NAME = "orders_write_ops"
TABLE_A_PATH = "/home/hudi/notebooks/warehouse/orders_write_ops"

if os.path.exists(TABLE_A_PATH):
    shutil.rmtree(TABLE_A_PATH)

base_options_a = {
    "hoodie.table.name": TABLE_A_NAME,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "order_date",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.index.type": "BLOOM",
}

initial_rows = [
    ("o1", "created",   100.0, "2026-09-01 10:00:00", "2026-09-01"),
    ("o2", "created",   250.0, "2026-09-01 10:05:00", "2026-09-01"),
    ("o3", "shipped",    75.0, "2026-09-01 10:10:00", "2026-09-01"),
    ("o4", "shipped",   300.0, "2026-09-01 10:15:00", "2026-09-01"),
    ("o5", "delivered",  50.0, "2026-09-01 10:20:00", "2026-09-01"),
]
df = spark.createDataFrame(initial_rows, cols)
df.write.format("hudi").options(**base_options_a).mode("overwrite").save(TABLE_A_PATH)
print(f"[hudi] TABLE A Step 1: initial write, {df.count()} rows (default operation=upsert, table was empty)")

# --- Step 2: operation=insert on an EXISTING key (o3) ---
insert_opts = dict(base_options_a, **{"hoodie.datasource.write.operation": "insert"})
insert_row = spark.createDataFrame([("o3", "shipped", 500.0, "2026-09-01 11:00:00", "2026-09-01")], cols)
insert_row.write.format("hudi").options(**insert_opts).mode("append").save(TABLE_A_PATH)

after_insert = spark.read.format("hudi").load(TABLE_A_PATH)
total_after_insert = after_insert.count()
o3_after_insert = after_insert.filter(after_insert.order_id == "o3").select("order_id", "amount").collect()
print(f"[hudi] TABLE A Step 2: operation=insert on existing key o3 (amount=500.0) -- total rows now {total_after_insert}")
print(f"[hudi] TABLE A Step 2: o3 now appears {len(o3_after_insert)} time(s): {o3_after_insert}")

# --- Step 3: operation=upsert on the SAME key, trying to "fix" the duplicate ---
upsert_opts = dict(base_options_a, **{"hoodie.datasource.write.operation": "upsert"})
upsert_row = spark.createDataFrame([("o3", "delivered", 700.0, "2026-09-01 12:00:00", "2026-09-01")], cols)
upsert_row.write.format("hudi").options(**upsert_opts).mode("append").save(TABLE_A_PATH)

after_upsert = spark.read.format("hudi").load(TABLE_A_PATH)
total_after_upsert = after_upsert.count()
o3_after_upsert = after_upsert.filter(after_upsert.order_id == "o3").select("order_id", "order_status", "amount").collect()
print(f"[hudi] TABLE A Step 3: operation=upsert on o3 (amount=700.0) -- total rows now {total_after_upsert}")
print(f"[hudi] TABLE A Step 3: o3 now appears {len(o3_after_upsert)} time(s): {o3_after_upsert}")
print("[hudi] TABLE A Step 3: (if this is still 2, upsert only reconciled the copy the index knew about --")
print("[hudi]   the duplicate insert-path copy stays stale forever unless separately deduped)")

# --- Step 4: operation=delete on o4 ---
delete_opts = dict(base_options_a, **{"hoodie.datasource.write.operation": "delete"})
delete_row = spark.createDataFrame([("o4", "shipped", 300.0, "2026-09-01 10:15:00", "2026-09-01")], cols)
delete_row.write.format("hudi").options(**delete_opts).mode("append").save(TABLE_A_PATH)

after_delete = spark.read.format("hudi").load(TABLE_A_PATH)
total_after_delete = after_delete.count()
o4_after_delete = after_delete.filter(after_delete.order_id == "o4").count()
print(f"[hudi] TABLE A Step 4: operation=delete on o4 -- total rows now {total_after_delete}, o4 count = {o4_after_delete}")

# =====================================================================
# TABLE B -- intra-batch duplicates with bulk_insert, with/without combine.before.insert
# =====================================================================
TABLE_B_NAME = "orders_bulk_load"
TABLE_B_PATH_1 = "/home/hudi/notebooks/warehouse/orders_bulk_load_no_combine"
TABLE_B_PATH_2 = "/home/hudi/notebooks/warehouse/orders_bulk_load_with_combine"
for p in (TABLE_B_PATH_1, TABLE_B_PATH_2):
    if os.path.exists(p):
        shutil.rmtree(p)

# Same key (b1) appears TWICE in the SAME incoming batch -- simulates a raw
# source file that itself has a duplicate row.
dup_batch = [
    ("b1", "created", 100.0, "2026-09-01 09:00:00", "2026-09-01"),
    ("b1", "created", 999.0, "2026-09-01 09:30:00", "2026-09-01"),  # later updated_at -- should "win" if combined
    ("b2", "created", 200.0, "2026-09-01 09:00:00", "2026-09-01"),
]
dup_df = spark.createDataFrame(dup_batch, cols)

bulk_opts_no_combine = {
    "hoodie.table.name": TABLE_B_NAME,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "order_date",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.combine.before.insert": "false",  # default -- no intra-batch dedup
}
dup_df.write.format("hudi").options(**bulk_opts_no_combine).mode("overwrite").save(TABLE_B_PATH_1)
no_combine_df = spark.read.format("hudi").load(TABLE_B_PATH_1)
b1_no_combine = no_combine_df.filter(no_combine_df.order_id == "b1").select("order_id", "amount").collect()
print(f"[hudi] TABLE B (combine.before.insert=false): b1 appears {len(b1_no_combine)} time(s): {b1_no_combine}")

bulk_opts_with_combine = dict(bulk_opts_no_combine, **{"hoodie.combine.before.insert": "true"})
dup_df.write.format("hudi").options(**bulk_opts_with_combine).mode("overwrite").save(TABLE_B_PATH_2)
with_combine_df = spark.read.format("hudi").load(TABLE_B_PATH_2)
b1_with_combine = with_combine_df.filter(with_combine_df.order_id == "b1").select("order_id", "amount").collect()
print(f"[hudi] TABLE B (combine.before.insert=true): b1 appears {len(b1_with_combine)} time(s): {b1_with_combine}")

print("[hudi] Phase 3 complete.")
