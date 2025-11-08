import psycopg2
from psycopg2 import sql, extras
import pandas as pd
import io
import os
from dotenv import load_dotenv

load_dotenv()

# Database connection details from connect_to_postgre.py
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def create_tables(cur):
    """Creates the dimension and fact tables in the data warehouse."""
    commands = (
        """
        DROP TABLE IF EXISTS fact_daily_rank CASCADE;
        """,
        """
        DROP TABLE IF EXISTS dim_song CASCADE;
        """,
        """
        DROP TABLE IF EXISTS dim_artist CASCADE;
        """,
        """
        DROP TABLE IF EXISTS dim_album CASCADE;
        """,
        """
        DROP TABLE IF EXISTS dim_date CASCADE;
        """,
        """
        DROP TABLE IF EXISTS dim_country CASCADE;
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_song (
            song_key SERIAL PRIMARY KEY,
            spotify_id VARCHAR(255) UNIQUE,
            name TEXT,
            is_explicit BOOLEAN,
            duration_ms INTEGER,
            danceability REAL,
            energy REAL,
            key INTEGER,
            loudness REAL,
            mode INTEGER,
            speechiness REAL,
            acousticness REAL,
            instrumentalness REAL,
            liveness REAL,
            valence REAL,
            tempo REAL,
            time_signature INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_artist (
            artist_key SERIAL PRIMARY KEY,
            artist_name TEXT UNIQUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_album (
            album_key SERIAL PRIMARY KEY,
            album_name TEXT,
            album_release_date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key SERIAL PRIMARY KEY,
            snapshot_date DATE UNIQUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_country (
            country_key SERIAL PRIMARY KEY,
            country_name VARCHAR(255) UNIQUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_daily_rank (
            song_key INTEGER REFERENCES dim_song(song_key),
            artist_key INTEGER REFERENCES dim_artist(artist_key),
            album_key INTEGER REFERENCES dim_album(album_key),
            date_key INTEGER REFERENCES dim_date(date_key),
            country_key INTEGER REFERENCES dim_country(country_key),
            daily_rank INTEGER,
            daily_movement INTEGER,
            weekly_movement INTEGER,
            popularity INTEGER,
            PRIMARY KEY (song_key, artist_key, date_key, country_key)
        )
        """
    )
    for command in commands:
        cur.execute(command)

def process_chunk(chunk, cur):
    """Processes a chunk of data and inserts it into the database."""

    # Insert into dim_artist
    artists = set(artist for artists_list in chunk['artists'].dropna() for artist in artists_list.split(', '))
    if artists:
        args_str = ','.join(cur.mogrify("(%s)", (artist,)).decode('utf-8') for artist in artists)
        cur.execute(f"INSERT INTO dim_artist (artist_name) VALUES {args_str} ON CONFLICT (artist_name) DO NOTHING")

    # Insert into dim_album
    albums = chunk[['album_name', 'album_release_date']].drop_duplicates().dropna()
    if not albums.empty:
        album_tuples = [tuple(x) for x in albums.to_numpy()]
        args_str = ','.join(cur.mogrify("(%s,%s)", x).decode('utf-8') for x in album_tuples)
        cur.execute(f"INSERT INTO dim_album (album_name, album_release_date) VALUES {args_str} ON CONFLICT DO NOTHING")

    # Insert into dim_date
    dates = chunk['snapshot_date'].drop_duplicates().dropna()
    if not dates.empty:
        date_tuples = [(date,) for date in dates]
        args_str = ','.join(cur.mogrify("(%s)", x).decode('utf-8') for x in date_tuples)
        cur.execute(f"INSERT INTO dim_date (snapshot_date) VALUES {args_str} ON CONFLICT (snapshot_date) DO NOTHING")

    # Insert into dim_country
    countries = chunk['country'].drop_duplicates().dropna()
    if not countries.empty:
        country_tuples = [(country,) for country in countries]
        args_str = ','.join(cur.mogrify("(%s)", x).decode('utf-8') for x in country_tuples)
        cur.execute(f"INSERT INTO dim_country (country_name) VALUES {args_str} ON CONFLICT (country_name) DO NOTHING")


    # Insert into dim_song
    songs = chunk[['spotify_id', 'name', 'is_explicit', 'duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature']].drop_duplicates('spotify_id').dropna(subset=['spotify_id'])
    if not songs.empty:
        song_tuples = [tuple(x) for x in songs.to_numpy()]
        extras.execute_values(cur,
            "INSERT INTO dim_song (spotify_id, name, is_explicit, duration_ms, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature) VALUES %s ON CONFLICT (spotify_id) DO NOTHING",
            song_tuples)


    # Get dimension keys
    cur.execute("SELECT spotify_id, song_key FROM dim_song")
    song_map = dict(cur.fetchall())
    cur.execute("SELECT artist_name, artist_key FROM dim_artist")
    artist_map = dict(cur.fetchall())
    cur.execute("SELECT album_name, album_key FROM dim_album")
    album_map = dict(cur.fetchall())
    cur.execute("SELECT snapshot_date, date_key FROM dim_date")
    date_map = {k.strftime('%Y-%m-%d'): v for k, v in cur.fetchall()}
    cur.execute("SELECT country_name, country_key FROM dim_country")
    country_map = dict(cur.fetchall())
    country_map[''] = country_map.get('', None) # Handle empty country string


    # Prepare fact table data
    facts = []
    for _, row in chunk.iterrows():
        # Because one song can have multiple artists, we will create a fact for each artist
        row_artists = [artist.strip() for artist in row['artists'].split(',')] if pd.notna(row['artists']) else []
        for artist_name in row_artists:
            song_key = song_map.get(row['spotify_id'])
            artist_key = artist_map.get(artist_name)
            album_key = album_map.get(row['album_name'])
            date_key = date_map.get(row['snapshot_date'])
            country_key = country_map.get(row['country'])

            if song_key and artist_key and album_key and date_key and country_key:
                facts.append((song_key, artist_key, album_key, date_key, country_key, row['daily_rank'], row['daily_movement'], row['weekly_movement'], row['popularity']))

    # Insert into fact_daily_rank
    if facts:
        extras.execute_values(cur,
            "INSERT INTO fact_daily_rank (song_key, artist_key, album_key, date_key, country_key, daily_rank, daily_movement, weekly_movement, popularity) VALUES %s ON CONFLICT DO NOTHING",
            facts)


def main():
    """Main function to create and populate the data warehouse."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()

        print("Creating tables...")
        create_tables(cur)
        conn.commit()
        print("Tables created successfully.")

        print("Processing CSV and loading data...")
        chunk_size = 10000
        for i, chunk in enumerate(pd.read_csv('universal_top_spotify_songs.csv', chunksize=chunk_size, iterator=True)):
            process_chunk(chunk, cur)
            conn.commit()
            print(f"Processed chunk {i+1}")

        print("Data loading complete.")

        cur.close()
        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
    main()
