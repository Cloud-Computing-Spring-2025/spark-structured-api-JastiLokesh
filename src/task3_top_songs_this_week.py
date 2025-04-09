from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, col, datediff, to_date, lit
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder.appName("Top Songs This Week").getOrCreate()

# Set log level to reduce verbose output
spark.sparkContext.setLogLevel("WARN")

# Load datasets
logs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/listening_logs.csv", header=True, inferSchema=True)
songs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/songs_metadata.csv", header=True, inferSchema=True)

# Convert timestamp to timestamp type for easier manipulation
logs_df = logs_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Join datasets to create enriched logs
enriched_logs = logs_df.join(songs_df, on="song_id", how="left")

reference_date = lit("2025-04-01") # Adjust this based on your data
print(f"\nUsing reference date: 2025-04-01")

# Filter for logs from the last 7 days before the reference date
current_week_logs = enriched_logs.filter(
    datediff(reference_date, to_date(col("timestamp"))) < 7
)

print(f"\nLogs from the last 7 days: {current_week_logs.count()}")

# Count plays for each song and join with song details
top_songs = current_week_logs.groupBy("song_id", "title", "artist", "genre") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10) \
    .select(
        col("song_id"),
        col("title"),
        col("artist"),
        col("genre"),
        col("count").alias("play_count")
    )



# Save result to CSV
top_songs.write.mode("overwrite").format("csv").option("header", "true").save("/workspaces/spark-structured-api-JastiLokesh/output/top_songs_this_week")
