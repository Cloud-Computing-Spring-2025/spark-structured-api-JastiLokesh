import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import uuid
import concurrent.futures
import itertools
from tqdm import tqdm  # For progress bar

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Define parameters
num_users = 1000
num_songs = 5000
num_listening_events = 100000

# Pre-define lists for faster lookups
genres = ['Pop', 'Rock', 'Jazz', 'Hip Hop', 'Classical', 'Electronic', 'Country', 'R&B', 'Indie', 'Metal']
moods = ['Happy', 'Sad', 'Energetic', 'Chill', 'Melancholic', 'Romantic', 'Angry', 'Peaceful']

title_words = ['The', 'A', 'My', 'Your', 'Our', 'Their', 'Her', 'His',
              'Love', 'Heart', 'Dream', 'Life', 'Night', 'Day', 'Time', 'Sky', 
              'Ocean', 'Mountain', 'River', 'Star', 'Sun', 'Moon', 'Fire', 'Water', 
              'Earth', 'Wind', 'Soul', 'Mind', 'Body', 'Dance', 'Song', 'Melody', 
              'Rhythm', 'Beat', 'Harmony', 'Journey', 'Adventure', 'Story', 'Tale', 
              'Fantasy', 'Reality', 'Memory', 'Hope', 'Faith', 'Trust', 'Belief', 
              'Doubt', 'Fear', 'Joy', 'Sorrow', 'Pain', 'Pleasure']

first_names = ['John', 'Jane', 'Michael', 'Emma', 'David', 'Sophia', 'Robert', 'Olivia',
               'William', 'Ava', 'James', 'Isabella', 'Benjamin', 'Mia', 'Lucas', 'Charlotte',
               'Henry', 'Amelia', 'Alexander', 'Harper', 'Sebastian', 'Evelyn', 'Jack',
               'Abigail', 'Daniel', 'Emily', 'Matthew', 'Elizabeth', 'Joseph', 'Sofia']

last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson',
             'Moore', 'Taylor', 'Anderson', 'Thomas', 'Jackson', 'White', 'Harris', 'Martin',
             'Thompson', 'Garcia', 'Martinez', 'Robinson', 'Clark', 'Rodriguez', 'Lewis',
             'Lee', 'Walker', 'Hall', 'Allen', 'Young', 'King', 'Wright']

band_prefixes = ['The', 'Electric', 'Royal', 'Mystic', 'Savage', 'Cosmic', 'Urban', 'Silent',
                'Midnight', 'Golden', 'Crystal', 'Velvet', 'Silver', 'Neon', 'Phantom', 'Wild']

band_suffixes = ['Dragons', 'Tigers', 'Eagles', 'Wolves', 'Lions', 'Bears', 'Hawks', 'Foxes',
                'Serpents', 'Panthers', 'Crows', 'Owls', 'Ghosts', 'Phantoms', 'Shadows', 'Spirits']

# Generate songs_metadata.csv
def generate_songs_metadata():
    print("Generating songs metadata...")
    
    # Pre-generate all UUIDs at once
    song_ids = [str(uuid.uuid4())[:8] for _ in range(num_songs)]
    
    # Generate random song titles and artists more efficiently
    titles = []
    artists = []
    
    # Generate multiple items concurrently
    def generate_batch(batch_size):
        batch_titles = []
        batch_artists = []
        
        for _ in range(batch_size):
            # Generate title
            title_length = random.randint(1, 3)
            title = ' '.join(random.sample(title_words, title_length))
            batch_titles.append(title)
            
            # Generate artist
            artist_type = random.randint(0, 2)
            if artist_type == 0:  # Single name
                artist = random.choice(first_names)
            elif artist_type == 1:  # Full name
                artist = f"{random.choice(first_names)} {random.choice(last_names)}"
            else:  # Band name
                artist = f"{random.choice(band_prefixes)} {random.choice(band_suffixes)}"
            batch_artists.append(artist)
        
        return batch_titles, batch_artists
    
    # Use multithreading to generate batches
    batch_size = 500
    num_batches = num_songs // batch_size + (1 if num_songs % batch_size > 0 else 0)
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        batch_results = list(executor.map(lambda _: generate_batch(batch_size), range(num_batches)))
    
    # Combine results
    for batch_titles, batch_artists in batch_results:
        titles.extend(batch_titles)
        artists.extend(batch_artists)
    
    # Trim to exact size
    titles = titles[:num_songs]
    artists = artists[:num_songs]
    
    # Efficiently generate random genres and moods
    random_genres = np.random.choice(genres, num_songs)
    random_moods = np.random.choice(moods, num_songs)
    
    # Create the dataframe
    songs_df = pd.DataFrame({
        'song_id': song_ids,
        'title': titles,
        'artist': artists,
        'genre': random_genres,
        'mood': random_moods
    })
    
    # Save to CSV
    songs_df.to_csv('/workspaces/assignment-3-spark-structured-api-JastiLokesh/input/songs_metadata.csv', index=False)
    print(f"Generated {num_songs} songs in songs_metadata.csv")
    
    return songs_df

# Generate listening_logs.csv
def generate_listening_logs(songs_df):
    print("Generating listening logs...")
    
    # Pre-generate all user IDs
    user_ids = [str(uuid.uuid4())[:8] for _ in range(num_users)]
    
    # Create a bias for each user towards certain genres and moods (more efficient)
    user_genre_preferences = {}
    user_mood_preferences = {}
    
    for user_id in user_ids:
        # Each user has 1-2 preferred genres with higher weights
        preferred_genres = np.random.choice(genres, size=random.randint(1, 2), replace=False)
        genre_weights = {genre: (random.uniform(0.6, 0.9) if genre in preferred_genres else random.uniform(0.1, 0.4)) 
                        for genre in genres}
        user_genre_preferences[user_id] = genre_weights
        
        # Each user has 1-2 preferred moods with higher weights
        preferred_moods = np.random.choice(moods, size=random.randint(1, 2), replace=False)
        mood_weights = {mood: (random.uniform(0.6, 0.9) if mood in preferred_moods else random.uniform(0.1, 0.4)) 
                       for mood in moods}
        user_mood_preferences[user_id] = mood_weights
    
    # End date is today
    end_date = datetime.now()
    # Start date is 30 days ago
    start_date = end_date - timedelta(days=30)
    total_seconds = (end_date - start_date).total_seconds()
    
    # Prepare song index for faster song selection
    # Create a mapping of genre and mood to songs
    genre_mood_to_songs = {}
    for genre in genres:
        for mood in moods:
            # Filter songs by genre and mood
            matching_songs = songs_df[(songs_df['genre'] == genre) & (songs_df['mood'] == mood)]
            if not matching_songs.empty:
                genre_mood_to_songs[(genre, mood)] = matching_songs['song_id'].tolist()
    
    # Generate listening events in batches
    batch_size = 10000
    num_batches = num_listening_events // batch_size + (1 if num_listening_events % batch_size > 0 else 0)
    all_logs = []
    
    def generate_log_batch(size):
        logs_batch = []
        
        for _ in range(size):
            # Choose a random user
            user_id = random.choice(user_ids)
            
            # Get user preferences
            user_genre_prefs = user_genre_preferences[user_id]
            user_mood_prefs = user_mood_preferences[user_id]
            
            # Weighted random selection of genre and mood
            genre = random.choices(genres, weights=[user_genre_prefs[g] for g in genres], k=1)[0]
            mood = random.choices(moods, weights=[user_mood_prefs[m] for m in moods], k=1)[0]
            
            # Select a song with this genre and mood
            if (genre, mood) in genre_mood_to_songs and genre_mood_to_songs[(genre, mood)]:
                song_id = random.choice(genre_mood_to_songs[(genre, mood)])
            else:
                # Fallback to a random song if no exact match found
                song_id = random.choice(songs_df['song_id'].tolist())
            
            # Generate timestamp
            time_offset = random.random() * total_seconds
            timestamp = start_date + timedelta(seconds=time_offset)
            
            # Generate duration
            full_duration = random.randint(120, 300)  # 2-5 minutes
            listen_percentage = 1.0 if random.random() > 0.2 else random.uniform(0.1, 0.9)
            duration_sec = int(full_duration * listen_percentage)
            
            logs_batch.append({
                'user_id': user_id,
                'song_id': song_id,
                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'duration_sec': duration_sec
            })
        
        return logs_batch
    
    # Generate logs in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(tqdm(
            executor.map(generate_log_batch, [batch_size] * num_batches),
            total=num_batches,
            desc="Generating listening logs"
        ))
    
    # Combine results
    for batch in results:
        all_logs.extend(batch)
    
    # Create the dataframe
    logs_df = pd.DataFrame(all_logs[:num_listening_events])  # Ensure exact count
    
    # Save to CSV
    logs_df.to_csv('/workspaces/assignment-3-spark-structured-api-JastiLokesh/input/listening_logs.csv', index=False)
    print(f"Generated {len(logs_df)} listening events in listening_logs.csv")

if __name__ == "__main__":
    # Record start time
    start_time = datetime.now()
    
    # Generate data
    songs_df = generate_songs_metadata()
    generate_listening_logs(songs_df)
    