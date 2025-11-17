# -*- coding: utf-8 -*-
"""
Spotify Data Warehouse - Visualization Dashboard
Tạo các biểu đồ thống kê từ database PostgreSQL
"""

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import rcParams
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Set up matplotlib for Vietnamese characters
rcParams['font.sans-serif'] = ['DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# Color scheme
colors = sns.color_palette("husl", 8)
sns.set_style("whitegrid")

def get_connection():
    """Kết nối đến PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("✓ Connected to database successfully")
        return conn
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return None

def query_to_dataframe(query, conn):
    """Execute query và convert kết quả thành DataFrame"""
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"✗ Query failed: {e}")
        return None

def create_visualizations(conn):
    """Tạo tất cả các biểu đồ"""
    
    # Tạo figure với subplots
    fig = plt.figure(figsize=(20, 24))
    fig.suptitle('Spotify Data Warehouse - Statistical Dashboard', 
                 fontsize=24, fontweight='bold', y=0.995)
    
    # ==================== 1. Top 10 Songs by Popularity ====================
    print("\n📊 Creating visualization 1: Top 10 Songs by Popularity...")
    ax1 = plt.subplot(4, 3, 1)
    
    query1 = """
    SELECT s.song_name, a.artist_name, AVG(fsd.popularity_score) as avg_popularity
    FROM fact_song_daily fsd
    JOIN dim_song s ON fsd.song_id = s.song_id
    JOIN dim_artist a ON s.song_id = a.artist_id
    GROUP BY s.song_name, a.artist_name
    ORDER BY avg_popularity DESC
    LIMIT 10
    """
    
    df1 = query_to_dataframe(query1, conn)
    if df1 is not None and not df1.empty:
        df1['label'] = df1['song_name'].str[:20] + '\n' + df1['artist_name'].str[:15]
        bars = ax1.barh(df1['label'], df1['avg_popularity'], color=colors[0])
        ax1.set_xlabel('Average Popularity Score', fontsize=10, fontweight='bold')
        ax1.set_title('Top 10 Most Popular Songs', fontsize=12, fontweight='bold')
        ax1.invert_yaxis()
        # Add value labels
        for i, v in enumerate(df1['avg_popularity']):
            ax1.text(v + 1, i, f'{v:.1f}', va='center', fontsize=8)
    
    # ==================== 2. Top 10 Artists ====================
    print("📊 Creating visualization 2: Top 10 Artists...")
    ax2 = plt.subplot(4, 3, 2)
    
    query2 = """
    SELECT a.artist_name, AVG(fas.artist_score) as avg_score, COUNT(DISTINCT fas.song_id) as num_songs
    FROM fact_artist_stats fas
    JOIN dim_artist a ON fas.artist_id = a.artist_id
    GROUP BY a.artist_name
    ORDER BY avg_score DESC
    LIMIT 10
    """
    
    df2 = query_to_dataframe(query2, conn)
    if df2 is not None and not df2.empty:
        bars = ax2.barh(df2['artist_name'].str[:20], df2['avg_score'], color=colors[1])
        ax2.set_xlabel('Average Artist Score', fontsize=10, fontweight='bold')
        ax2.set_title('Top 10 Artists by Score', fontsize=12, fontweight='bold')
        ax2.invert_yaxis()
        for i, v in enumerate(df2['avg_score']):
            ax2.text(v + 0.1, i, f'{v:.1f}', va='center', fontsize=8)
    
    # ==================== 3. Songs Distribution by Energy Level ====================
    print("📊 Creating visualization 3: Energy Level Distribution...")
    ax3 = plt.subplot(4, 3, 3)
    
    query3 = """
    SELECT daf.energy_level, COUNT(DISTINCT faa.song_id) as count
    FROM fact_audio_analysis faa
    JOIN dim_audio_features daf ON faa.features_id = daf.features_id
    GROUP BY daf.energy_level
    ORDER BY 
        CASE daf.energy_level
            WHEN 'Very Low' THEN 1
            WHEN 'Low' THEN 2
            WHEN 'Medium' THEN 3
            WHEN 'High' THEN 4
            WHEN 'Very High' THEN 5
        END
    """
    
    df3 = query_to_dataframe(query3, conn)
    if df3 is not None and not df3.empty:
        wedges, texts, autotexts = ax3.pie(df3['count'], labels=df3['energy_level'], 
                                            autopct='%1.1f%%', colors=colors)
        ax3.set_title('Songs Distribution by Energy Level', fontsize=12, fontweight='bold')
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(9)
            autotext.set_fontweight('bold')
    
    # ==================== 4. Danceability Distribution ====================
    print("📊 Creating visualization 4: Danceability Distribution...")
    ax4 = plt.subplot(4, 3, 4)
    
    query4 = """
    SELECT daf.danceability_level, COUNT(DISTINCT faa.song_id) as count
    FROM fact_audio_analysis faa
    JOIN dim_audio_features daf ON faa.features_id = daf.features_id
    GROUP BY daf.danceability_level
    ORDER BY 
        CASE daf.danceability_level
            WHEN 'Very Low' THEN 1
            WHEN 'Low' THEN 2
            WHEN 'Medium' THEN 3
            WHEN 'High' THEN 4
            WHEN 'Very High' THEN 5
        END
    """
    
    df4 = query_to_dataframe(query4, conn)
    if df4 is not None and not df4.empty:
        wedges, texts, autotexts = ax4.pie(df4['count'], labels=df4['danceability_level'], 
                                            autopct='%1.1f%%', colors=colors)
        ax4.set_title('Songs Distribution by Danceability', fontsize=12, fontweight='bold')
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(9)
            autotext.set_fontweight('bold')
    
    # ==================== 5. Valence (Mood) Distribution ====================
    print("📊 Creating visualization 5: Mood (Valence) Distribution...")
    ax5 = plt.subplot(4, 3, 5)
    
    query5 = """
    SELECT daf.valence_level, COUNT(DISTINCT faa.song_id) as count
    FROM fact_audio_analysis faa
    JOIN dim_audio_features daf ON faa.features_id = daf.features_id
    GROUP BY daf.valence_level
    ORDER BY 
        CASE daf.valence_level
            WHEN 'Very Negative' THEN 1
            WHEN 'Negative' THEN 2
            WHEN 'Neutral' THEN 3
            WHEN 'Positive' THEN 4
            WHEN 'Very Positive' THEN 5
        END
    """
    
    df5 = query_to_dataframe(query5, conn)
    if df5 is not None and not df5.empty:
        bars = ax5.bar(df5['valence_level'], df5['count'], color=colors[2])
        ax5.set_xlabel('Mood Level', fontsize=10, fontweight='bold')
        ax5.set_ylabel('Number of Songs', fontsize=10, fontweight='bold')
        ax5.set_title('Songs Distribution by Mood (Valence)', fontsize=12, fontweight='bold')
        ax5.tick_params(axis='x', rotation=45)
        for i, v in enumerate(df5['count']):
            ax5.text(i, v + 10, str(v), ha='center', fontsize=9, fontweight='bold')
    
    # ==================== 6. Acousticness Distribution ====================
    print("📊 Creating visualization 6: Acousticness Distribution...")
    ax6 = plt.subplot(4, 3, 6)
    
    query6 = """
    SELECT daf.acousticness_level, COUNT(DISTINCT faa.song_id) as count
    FROM fact_audio_analysis faa
    JOIN dim_audio_features daf ON faa.features_id = daf.features_id
    GROUP BY daf.acousticness_level
    ORDER BY 
        CASE daf.acousticness_level
            WHEN 'Very Low' THEN 1
            WHEN 'Low' THEN 2
            WHEN 'Medium' THEN 3
            WHEN 'High' THEN 4
            WHEN 'Very High' THEN 5
        END
    """
    
    df6 = query_to_dataframe(query6, conn)
    if df6 is not None and not df6.empty:
        bars = ax6.bar(df6['acousticness_level'], df6['count'], color=colors[3])
        ax6.set_xlabel('Acousticness Level', fontsize=10, fontweight='bold')
        ax6.set_ylabel('Number of Songs', fontsize=10, fontweight='bold')
        ax6.set_title('Songs Distribution by Acousticness', fontsize=12, fontweight='bold')
        ax6.tick_params(axis='x', rotation=45)
        for i, v in enumerate(df6['count']):
            ax6.text(i, v + 10, str(v), ha='center', fontsize=9, fontweight='bold')
    
    # ==================== 7. Tempo Category Distribution ====================
    print("📊 Creating visualization 7: Tempo Distribution...")
    ax7 = plt.subplot(4, 3, 7)
    
    query7 = """
    SELECT daf.tempo_category, COUNT(DISTINCT faa.song_id) as count
    FROM fact_audio_analysis faa
    JOIN dim_audio_features daf ON faa.features_id = daf.features_id
    GROUP BY daf.tempo_category
    ORDER BY 
        CASE daf.tempo_category
            WHEN 'Very Slow' THEN 1
            WHEN 'Slow' THEN 2
            WHEN 'Moderate' THEN 3
            WHEN 'Fast' THEN 4
            WHEN 'Very Fast' THEN 5
        END
    """
    
    df7 = query_to_dataframe(query7, conn)
    if df7 is not None and not df7.empty:
        bars = ax7.bar(df7['tempo_category'], df7['count'], color=colors[4])
        ax7.set_xlabel('Tempo Category', fontsize=10, fontweight='bold')
        ax7.set_ylabel('Number of Songs', fontsize=10, fontweight='bold')
        ax7.set_title('Songs Distribution by Tempo', fontsize=12, fontweight='bold')
        ax7.tick_params(axis='x', rotation=45)
        for i, v in enumerate(df7['count']):
            ax7.text(i, v + 10, str(v), ha='center', fontsize=9, fontweight='bold')
    
    # ==================== 8. Top 10 Countries by Song Count ====================
    print("📊 Creating visualization 8: Top Countries...")
    ax8 = plt.subplot(4, 3, 8)
    
    query8 = """
    SELECT c.country_name, COUNT(DISTINCT fsd.song_id) as song_count
    FROM fact_song_daily fsd
    JOIN dim_country c ON fsd.country_id = c.country_id
    WHERE c.country_code != 'GLOBAL'
    GROUP BY c.country_name
    ORDER BY song_count DESC
    LIMIT 10
    """
    
    df8 = query_to_dataframe(query8, conn)
    if df8 is not None and not df8.empty:
        bars = ax8.barh(df8['country_name'], df8['song_count'], color=colors[5])
        ax8.set_xlabel('Number of Songs', fontsize=10, fontweight='bold')
        ax8.set_title('Top 10 Countries by Song Count', fontsize=12, fontweight='bold')
        ax8.invert_yaxis()
        for i, v in enumerate(df8['song_count']):
            ax8.text(v + 20, i, str(v), va='center', fontsize=9, fontweight='bold')
    
    # ==================== 9. Average Popularity by Country (Top 10) ====================
    print("📊 Creating visualization 9: Avg Popularity by Country...")
    ax9 = plt.subplot(4, 3, 9)
    
    query9 = """
    SELECT c.country_name, AVG(fsd.popularity_score) as avg_popularity
    FROM fact_song_daily fsd
    JOIN dim_country c ON fsd.country_id = c.country_id
    WHERE c.country_code != 'GLOBAL'
    GROUP BY c.country_name
    ORDER BY avg_popularity DESC
    LIMIT 10
    """
    
    df9 = query_to_dataframe(query9, conn)
    if df9 is not None and not df9.empty:
        bars = ax9.barh(df9['country_name'], df9['avg_popularity'], color=colors[6])
        ax9.set_xlabel('Average Popularity Score', fontsize=10, fontweight='bold')
        ax9.set_title('Top 10 Countries by Avg Popularity', fontsize=12, fontweight='bold')
        ax9.invert_yaxis()
        for i, v in enumerate(df9['avg_popularity']):
            ax9.text(v + 1, i, f'{v:.1f}', va='center', fontsize=8)
    
    # ==================== 10. Energy vs Danceability Scatter ====================
    print("📊 Creating visualization 10: Energy vs Danceability...")
    ax10 = plt.subplot(4, 3, 10)
    
    query10 = """
    SELECT faa.energy, faa.danceability, COUNT(*) as count
    FROM fact_audio_analysis faa
    GROUP BY faa.energy, faa.danceability
    LIMIT 100
    """
    
    df10 = query_to_dataframe(query10, conn)
    if df10 is not None and not df10.empty:
        scatter = ax10.scatter(df10['energy'], df10['danceability'], 
                              s=df10['count']*5, alpha=0.6, c=df10['count'], 
                              cmap='viridis')
        ax10.set_xlabel('Energy', fontsize=10, fontweight='bold')
        ax10.set_ylabel('Danceability', fontsize=10, fontweight='bold')
        ax10.set_title('Energy vs Danceability Correlation', fontsize=12, fontweight='bold')
        plt.colorbar(scatter, ax=ax10, label='Count')
    
    # ==================== 11. Valence vs Energy Scatter ====================
    print("📊 Creating visualization 11: Valence vs Energy...")
    ax11 = plt.subplot(4, 3, 11)
    
    query11 = """
    SELECT faa.valence, faa.energy, COUNT(*) as count
    FROM fact_audio_analysis faa
    GROUP BY faa.valence, faa.energy
    LIMIT 100
    """
    
    df11 = query_to_dataframe(query11, conn)
    if df11 is not None and not df11.empty:
        scatter = ax11.scatter(df11['valence'], df11['energy'], 
                              s=df11['count']*5, alpha=0.6, c=df11['count'], 
                              cmap='plasma')
        ax11.set_xlabel('Valence (Mood)', fontsize=10, fontweight='bold')
        ax11.set_ylabel('Energy', fontsize=10, fontweight='bold')
        ax11.set_title('Valence vs Energy Correlation', fontsize=12, fontweight='bold')
        plt.colorbar(scatter, ax=ax11, label='Count')
    
    # ==================== 12. Tempo Distribution Histogram ====================
    print("📊 Creating visualization 12: Tempo Distribution...")
    ax12 = plt.subplot(4, 3, 12)
    
    query12 = """
    SELECT faa.tempo
    FROM fact_audio_analysis faa
    WHERE faa.tempo > 0 AND faa.tempo < 250
    """
    
    df12 = query_to_dataframe(query12, conn)
    if df12 is not None and not df12.empty:
        ax12.hist(df12['tempo'], bins=30, color=colors[7], edgecolor='black', alpha=0.7)
        ax12.set_xlabel('Tempo (BPM)', fontsize=10, fontweight='bold')
        ax12.set_ylabel('Number of Songs', fontsize=10, fontweight='bold')
        ax12.set_title('Tempo Distribution (BPM)', fontsize=12, fontweight='bold')
        ax12.axvline(df12['tempo'].mean(), color='red', linestyle='--', 
                     linewidth=2, label=f"Mean: {df12['tempo'].mean():.1f}")
        ax12.axvline(df12['tempo'].median(), color='green', linestyle='--', 
                     linewidth=2, label=f"Median: {df12['tempo'].median():.1f}")
        ax12.legend(fontsize=8)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save figure
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"spotify_dashboard_{timestamp}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"\n✓ Dashboard saved as: {filename}")
    
    plt.show()

def print_statistics(conn):
    """In thống kê database"""
    print("\n" + "="*80)
    print("  📊 SPOTIFY DATA WAREHOUSE - DATABASE STATISTICS")
    print("="*80)
    
    cur = conn.cursor()
    
    # Statistics queries
    stats = [
        ("Total Songs", "SELECT COUNT(DISTINCT song_id) FROM dim_song"),
        ("Total Artists", "SELECT COUNT(DISTINCT artist_id) FROM dim_artist"),
        ("Total Albums", "SELECT COUNT(DISTINCT album_id) FROM dim_album"),
        ("Total Countries", "SELECT COUNT(DISTINCT country_id) FROM dim_country WHERE country_code != 'GLOBAL'"),
        ("Date Range", "SELECT MIN(full_date) || ' to ' || MAX(full_date) FROM dim_date"),
        ("Total Song Daily Records", "SELECT COUNT(*) FROM fact_song_daily"),
        ("Total Artist Stats Records", "SELECT COUNT(*) FROM fact_artist_stats"),
        ("Total Chart Position Records", "SELECT COUNT(*) FROM fact_chart_position"),
        ("Total Audio Analysis Records", "SELECT COUNT(*) FROM fact_audio_analysis"),
        ("Total Streaming Metrics Records", "SELECT COUNT(*) FROM fact_streaming_metrics"),
    ]
    
    for stat_name, query in stats:
        try:
            cur.execute(query)
            result = cur.fetchone()[0]
            print(f"  {stat_name:.<50} {str(result):>20}")
        except Exception as e:
            print(f"  {stat_name:.<50} Error: {e}")
    
    print("="*80 + "\n")
    cur.close()

def main():
    """Main function"""
    print("\n" + "="*80)
    print("  🎵 SPOTIFY DATA WAREHOUSE - VISUALIZATION GENERATOR")
    print("="*80 + "\n")
    
    # Connect to database
    conn = get_connection()
    if conn is None:
        print("✗ Cannot connect to database. Exiting.")
        return
    
    try:
        # Print statistics
        print_statistics(conn)
        
        # Create visualizations
        print("🎨 Generating 12-panel statistical dashboard...\n")
        create_visualizations(conn)
        
        print("\n✅ All visualizations completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
    finally:
        conn.close()
        print("✓ Database connection closed.")

if __name__ == "__main__":
    main()
