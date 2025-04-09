from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("Happy Song Recommendations").getOrCreate()

# Set log level to reduce verbose output
spark.sparkContext.setLogLevel("WARN")

# Load datasets
logs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/listening_logs.csv", header=True, inferSchema=True)
songs_df = spark.read.csv("/workspaces/spark-structured-api-JastiLokesh/input/songs_metadata.csv", header=True, inferSchema=True)


# Check if we have any songs with "Happy" mood
happy_count = songs_df.filter(col("mood") == "Happy").count()
print(f"\nHappy songs count: {happy_count}")

# Check if we have any songs with "Sad" mood
sad_count = songs_df.filter(col("mood") == "Sad").count()
print(f"\nSad songs count: {sad_count}")

# Convert timestamp to timestamp type for easier manipulation
logs_df = logs_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Join datasets to create enriched logs
enriched_logs = logs_df.join(songs_df, on="song_id", how="left")

# Get unique users
unique_users = logs_df.select("user_id").distinct()
print(f"\nTotal unique users: {unique_users.count()}")

mood_counts = enriched_logs.groupBy("user_id", "mood").count()

# Get total count of songs played by each user
total_counts = mood_counts.groupBy("user_id").agg(sum("count").alias("total_count"))

# Join to get proportion of each mood per user
mood_proportions = mood_counts.join(total_counts, on="user_id").withColumn("proportion", col("count") / col("total_count"))

# Filter for users who primarily listen to "Sad" songs (proportion > 0.3)
sad_listeners = mood_proportions.filter((col("mood") == "Sad") & (col("proportion") > 0.3)).select("user_id")
print(f"\nSad listeners (>30% sad songs): {sad_listeners.count()}")

# If no sad listeners found, get users with highest proportion of sad songs
if sad_listeners.count() == 0:
    sad_listeners = mood_proportions.filter(col("mood") == "Sad").orderBy(desc("proportion")).select("user_id").limit(3)
    print(f"\nSad listeners (top 3 by proportion): {sad_listeners.count()}")

# If still no sad listeners, use any random users
if sad_listeners.count() == 0:
    sad_listeners = unique_users.limit(3)
    print("\nNo sad listeners found, using 3 random users")

# Get the user IDs
sad_user_ids = [r.user_id for r in sad_listeners.collect()]
print(f"\nSelected user IDs: {sad_user_ids}")

# Find songs that these users have listened to
listened_songs = enriched_logs.filter(col("user_id").isin(sad_user_ids)).select("user_id", "song_id").distinct()

# Find "Happy" songs

happy_songs = songs_df.filter(col("mood") == "Happy")
print(f"\nHappy songs available for recommendations: {happy_songs.count()}")

# If no happy songs found, use random songs
if happy_songs.count() == 0:
    happy_songs = songs_df.limit(5)
    print("\nNo happy songs found, using 5 random songs instead")

# Initialize empty DataFrame for all recommendations
all_recommendations = None

# For each user, recommend up to 3 happy songs they haven't listened to
for user_id in sad_user_ids:
    # Get songs this user has already listened to
    user_listened = listened_songs.filter(col("user_id") == user_id)
    user_listened_songs = [r.song_id for r in user_listened.select("song_id").collect()]
    

    # Filter happy songs not listened to by this user
    user_recommendations = happy_songs.filter(~col("song_id").isin(user_listened_songs))
    
    # Limit to 3 songs
    user_recommendations = user_recommendations.limit(3)
    
    # Add user_id column
    user_recommendations = user_recommendations.withColumn("user_id", lit(user_id))
    
    # Select relevant columns
    user_recommendations = user_recommendations.select("user_id", "song_id", "title", "artist", "genre")
    
    recommendation_count = user_recommendations.count()
    
    # If user has heard all happy songs, recommend any songs
    if recommendation_count == 0:
        print(f"User {user_id} has heard all happy songs, recommending any songs")
        user_recommendations = songs_df.filter(~col("song_id").isin(user_listened_songs)).limit(3)
        
        # If still no recommendations, just use any songs
        if user_recommendations.count() == 0:
            print(f"User {user_id} has heard all songs, recommending random songs")
            user_recommendations = songs_df.limit(3)
        
        user_recommendations = user_recommendations.withColumn("user_id", lit(user_id)).select("user_id", "song_id", "title", "artist", "genre")
    
    # Add to all recommendations
    if all_recommendations is None:
        all_recommendations = user_recommendations
    else:
        all_recommendations = all_recommendations.union(user_recommendations)


# Save result to CSV as a single file
all_recommendations.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save("/workspaces/spark-structured-api-JastiLokesh/output/happy_recommendations")
