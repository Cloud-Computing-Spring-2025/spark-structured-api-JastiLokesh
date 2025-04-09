from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, rank, col
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("User Favorite Genres").getOrCreate()

# Set log level to reduce verbose output
spark.sparkContext.setLogLevel("WARN")

# Load datasets
logs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/listening_logs.csv", header=True, inferSchema=True)
songs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/songs_metadata.csv", header=True, inferSchema=True)

# Convert timestamp to timestamp type for easier manipulation
logs_df = logs_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Join datasets to create enriched logs
enriched_logs = logs_df.join(songs_df, on="song_id", how="left")

# Count the number of plays per genre for each user
genre_counts = enriched_logs.groupBy("user_id", "genre").count()

# Define a window specification to rank genres for each user by play count
window_spec = Window.partitionBy("user_id").orderBy(desc("count"))

# Rank genres for each user
ranked_genres = genre_counts.withColumn("rank", rank().over(window_spec))

# Filter to get only the top genre for each user
favorite_genres = ranked_genres.filter(col("rank") == 1).select(
    col("user_id"), 
    col("genre").alias("favorite_genre"), 
    col("count").alias("play_count")
)

# Save result to CSV
favorite_genres.write.mode("overwrite").format("csv").option("header", "true").save("/workspaces/spark-structured-api-JastiLokesh/output/user_favorite_genres")