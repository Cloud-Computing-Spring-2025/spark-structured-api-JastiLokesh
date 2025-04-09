from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc, col

# Initialize Spark Session
spark = SparkSession.builder.appName("Average Listen Time Per Song").getOrCreate()

# Set log level to reduce verbose output
spark.sparkContext.setLogLevel("WARN")

# Load datasets
logs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/listening_logs.csv", header=True, inferSchema=True)
songs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/songs_metadata.csv", header=True, inferSchema=True)

# Calculate average duration per song
avg_listen_time = logs_df.groupBy("song_id") \
    .agg(
        avg("duration_sec").alias("avg_duration_sec"),
        count("*").alias("play_count")
    )

# Join with song metadata to include song title and artist
avg_listen_time_with_details = avg_listen_time.join(
    songs_df.select("song_id", "title", "artist"),
    on="song_id",
    how="left"
).orderBy(desc("play_count"))

# Save result to CSV
avg_listen_time_with_details.write.mode("overwrite").format("csv").option("header", "true").save("/workspaces/spark-structured-api-JastiLokesh/output/avg_listen_time_per_song")