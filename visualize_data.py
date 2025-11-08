import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt

load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def visualize_top_artists():
    """
    Connects to the data warehouse, retrieves top artists, and visualizes the data.
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

        query = """
        SELECT
            da.artist_name,
            COUNT(fdr.song_key) AS song_count
        FROM
            fact_daily_rank fdr
        JOIN
            dim_artist da ON fdr.artist_key = da.artist_key
        JOIN
            dim_country dc ON fdr.country_key = dc.country_key
        WHERE
            dc.country_name = 'JP'
        GROUP BY
            da.artist_name
        ORDER BY
            song_count DESC
        LIMIT 20; -- Increased limit to 20 artists
        """
        df = pd.read_sql(query, conn)

        if not df.empty:
            plt.figure(figsize=(14, 8)) # Increased figure size
            # Use a different colormap for better aesthetics
            colors = plt.cm.viridis(df['song_count'] / float(max(df['song_count'])))
            plt.bar(df['artist_name'], df['song_count'], color=colors)
            plt.xlabel('Artist Name', fontsize=12)
            plt.ylabel('Number of Top Tracks', fontsize=12)
            plt.title('Top 20 Japanese Artists by Number of Top Tracks', fontsize=14) # Updated title
            plt.xticks(rotation=60, ha='right', fontsize=10) # Increased rotation for more artists
            plt.yticks(fontsize=10)
            plt.grid(axis='y', linestyle='--', alpha=0.7) # Add grid lines

            # Note: For proper display of Japanese characters, you might need to configure
            # matplotlib to use a font that supports them (e.g., 'Meiryo', 'Yu Gothic').
            # Example: plt.rcParams['font.family'] = 'Yu Gothic' (if installed)

            plt.tight_layout()
            plt.show()
        else:
            print("No data found for top artists.")

    except psycopg2.OperationalError as e:
        print(f"❌ Database connection error: {e}")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    visualize_top_artists()
