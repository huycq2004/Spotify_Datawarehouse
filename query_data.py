import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def get_jp_songs_and_artists():
    """
    Connects to the data warehouse and retrieves information about
    songs and artists from Japan.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()

        query = """
        SELECT
            ds.name AS song_name,
            da.artist_name,
            fdr.daily_rank,
            fdr.popularity,
            dd.snapshot_date
        FROM
            fact_daily_rank fdr
        JOIN
            dim_song ds ON fdr.song_key = ds.song_key
        JOIN
            dim_artist da ON fdr.artist_key = da.artist_key
        JOIN
            dim_country dc ON fdr.country_key = dc.country_key
        JOIN
            dim_date dd ON fdr.date_key = dd.date_key
        WHERE
            dc.country_name = 'JP'
        ORDER BY
            dd.snapshot_date DESC, fdr.daily_rank ASC
        LIMIT 20;
        """
        cur.execute(query)
        results = cur.fetchall()

        print("Songs and Artists from Japan (JP):")
        print("-" * 50)
        for row in results:
            print(f"Date: {row[4]}, Rank: {row[2]}, Song: {row[0]}, Artist: {row[1]}, Popularity: {row[3]}")
        print("-" * 50)

        cur.close()

    except psycopg2.OperationalError as e:
        print(f"❌ Database connection error: {e}")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    get_jp_songs_and_artists()
