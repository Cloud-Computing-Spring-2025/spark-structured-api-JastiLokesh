from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour

# Initialize Spark Session
spark = SparkSession.builder.appName("Night Owl Users").getOrCreate()

# Set log level to reduce verbose output
spark.sparkContext.setLogLevel("WARN")

# Load datasets
logs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/listening_logs.csv", header=True, inferSchema=True)
songs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/songs_metadata.csv", header=True, inferSchema=True)

# Convert timestamp to timestamp type for easier manipulation
logs_df = logs_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Join datasets to create enriched logs
enriched_logs = logs_df.join(songs_df, on="song_id", how="left")

# Extract hour from timestamp
logs_with_hour = enriched_logs.withColumn("hour", hour(col("timestamp")))

# Filter logs for late night hours (0-4, which is 12 AM to 5 AM)
night_logs = logs_with_hour.filter((col("hour") >= 0) & (col("hour") < 5))

# Count all plays for each user
total_plays = enriched_logs.groupBy("user_id").count().withColumnRenamed("count", "total_plays")

# Count night plays for each user
night_plays = night_logs.groupBy("user_id").count().withColumnRenamed("count", "night_plays")

# Join to calculate proportion of night plays
night_owl_users = night_plays.join(total_plays, on="user_id") \
    .withColumn("night_play_ratio", col("night_plays") / col("total_plays")) \
    .orderBy(col("night_play_ratio").desc())

# Filter for users with > 30% of plays during night hours
frequent_night_owls = night_owl_users.filter(col("night_play_ratio") > 0.3)

# Save result to CSV
frequent_night_owls.write.mode("overwrite").format("csv").option("header", "true").save("/workspaces/spark-structured-api-JastiLokesh/output/night_owl_users")