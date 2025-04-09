from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, desc
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Genre Loyalty Scores").getOrCreate()

# Set log level to reduce verbose output
spark.sparkContext.setLogLevel("WARN")

# Load datasets
logs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/listening_logs.csv", header=True, inferSchema=True)
songs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/songs_metadata.csv", header=True, inferSchema=True)

# Convert timestamp to timestamp type for easier manipulation
logs_df = logs_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Join datasets to create enriched logs
enriched_logs = logs_df.join(songs_df, on="song_id", how="left")

# Count plays per genre for each user
genre_counts = enriched_logs.groupBy("user_id", "genre").count()

# Get total plays for each user
total_plays = enriched_logs.groupBy("user_id").count().withColumnRenamed("count", "total_plays")

# Join to calculate proportion
user_genre_proportions = genre_counts.join(total_plays, on="user_id").withColumn("proportion", col("count") / col("total_plays"))

# Get the maximum proportion for each user (loyalty score)
window_spec = Window.partitionBy("user_id")
loyalty_scores = user_genre_proportions.withColumn(
    "max_proportion", spark_max("proportion").over(window_spec)
).filter(col("proportion") == col("max_proportion")) \
    .select(
        col("user_id"),
        col("genre").alias("favorite_genre"),
        col("max_proportion").alias("loyalty_score"),
        col("count").alias("plays_in_genre"),
        col("total_plays")
    )

# Calculate appropriate threshold based on data distribution
median_loyalty = float(loyalty_scores.select("loyalty_score").summary("50%").collect()[0][1])
threshold = max(0.33, median_loyalty * 1.1)

# Filter for users with loyalty score above the threshold
loyal_users = loyalty_scores.filter(col("loyalty_score") >= threshold)

# If too few users, fall back to top users
if loyal_users.count() < 5:
    loyal_users = loyalty_scores.orderBy(desc("loyalty_score")).limit(10)

# Save result to CSV
loyal_users.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save("/workspaces/spark-structured-api-JastiLokesh/output/genre_loyalty_scores")