from pyspark.sql import functions as F
import os, glob
from datetime import datetime, timedelta

catalog = "calgary_transit"
schema = "bronze"
table_prefix = "gtfs_"

start_date = datetime.strptime("2026-02-10", "%Y-%m-%d").date()
end_date   = datetime.strptime("2026-02-18", "%Y-%m-%d").date()

ingest_ts = F.current_timestamp()

d = start_date
while d <= end_date:
    date_str = d.strftime("%Y-%m-%d")
    extract_dir = f"/Volumes/calgary_transit/bronze/bronze_vol/calgary_gtfs/{date_str}/"
    txt_files = glob.glob(os.path.join(extract_dir, "*.txt"))

    if not txt_files:
        print(f"⚠️ No .txt files for {date_str} — skipping")
        d += timedelta(days=1)
        continue

    for fp in sorted(txt_files):
        base = os.path.splitext(os.path.basename(fp))[0]
        table_name = f"{catalog}.{schema}.{table_prefix}{base}"

        df = (
            spark.read.option("header", True).option("inferSchema", True)
            .option("mode", "PERMISSIVE")
            .csv(fp)
            .withColumn("_ingest_date", F.lit(date_str))
            .withColumn("_ingest_ts", ingest_ts)
            .withColumn("_source_file", F.lit(fp))
        )

        (df.write.format("delta").mode("append")
         .option("mergeSchema", "true")
         .saveAsTable(table_name))

        print(f"✅ {date_str}: appended -> {table_name}")

    d += timedelta(days=1)
