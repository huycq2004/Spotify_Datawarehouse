import psycopg2
from psycopg2 import sql, extras
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from datetime import datetime
import re

load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

"""
================================================================================
SPOTIFY DATA WAREHOUSE - STUDENT PROJECT VERSION
================================================================================
Thiết kế: Constellation Schema với 11 bảng
- 6 Dimension Tables
- 5 Fact Tables
Phù hợp cho: Đồ án môn Kho Dữ Liệu, nhóm 4-5 sinh viên
================================================================================
"""

# ========================================
# PHẦN 1: DATA CLEANING (ETL - TRANSFORM)
# ========================================

def clean_text(text):
    """Làm sạch và chuẩn hóa text"""
    if pd.isna(text) or text == '':
        return None
    text = str(text).strip()
    text = re.sub(r'\s+', ' ', text)
    return text if text else None

def clean_numeric(value, min_val=None, max_val=None, default=None):
    """Làm sạch và validate số"""
    if pd.isna(value):
        return default
    try:
        value = float(value)
        if min_val is not None and value < min_val:
            return default
        if max_val is not None and value > max_val:
            return default
        return value
    except:
        return default

def clean_boolean(value):
    """Làm sạch boolean"""
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    value_str = str(value).lower()
    return value_str in ['true', '1', 'yes', 't']

def clean_date(date_str):
    """Làm sạch và validate date"""
    if pd.isna(date_str) or date_str == '':
        return None
    try:
        return pd.to_datetime(date_str).date()
    except:
        return None

def extract_and_clean_artists(artists_str):
    """Tách và làm sạch danh sách nghệ sĩ"""
    if pd.isna(artists_str) or artists_str == '':
        return []
    artists = [clean_text(artist) for artist in str(artists_str).split(',')]
    return [a for a in artists if a is not None]

def categorize_mood(valence, energy):
    """Phân loại mood dựa trên valence và energy"""
    if valence >= 0.6 and energy >= 0.6:
        return 'Happy'
    elif valence >= 0.6 and energy < 0.6:
        return 'Calm'
    elif valence < 0.4 and energy >= 0.6:
        return 'Energetic'
    elif valence < 0.4 and energy < 0.6:
        return 'Sad'
    else:
        return 'Neutral'

def categorize_audio_features(energy, danceability, valence, acousticness, tempo):
    """
    Phân loại đặc điểm audio thành các level categories
    Returns: tuple (energy_level, danceability_level, valence_level, tempo_category, acousticness_level)
    """
    # Convert to float with defaults
    energy = float(energy) if energy else 0.5
    danceability = float(danceability) if danceability else 0.5
    valence = float(valence) if valence else 0.5
    acousticness = float(acousticness) if acousticness else 0.5
    tempo = float(tempo) if tempo else 120.0
    
    # Categorize Energy Level
    if energy >= 0.8:
        energy_level = 'Very High'
    elif energy >= 0.6:
        energy_level = 'High'
    elif energy >= 0.4:
        energy_level = 'Medium'
    elif energy >= 0.2:
        energy_level = 'Low'
    else:
        energy_level = 'Very Low'
    
    # Categorize Danceability Level
    if danceability >= 0.8:
        danceability_level = 'Very High'
    elif danceability >= 0.6:
        danceability_level = 'High'
    elif danceability >= 0.4:
        danceability_level = 'Medium'
    elif danceability >= 0.2:
        danceability_level = 'Low'
    else:
        danceability_level = 'Very Low'
    
    # Categorize Valence (Mood) Level
    if valence >= 0.8:
        valence_level = 'Very Positive'
    elif valence >= 0.6:
        valence_level = 'Positive'
    elif valence >= 0.4:
        valence_level = 'Neutral'
    elif valence >= 0.2:
        valence_level = 'Negative'
    else:
        valence_level = 'Very Negative'
    
    # Categorize Tempo
    if tempo >= 140:
        tempo_category = 'Very Fast'
    elif tempo >= 120:
        tempo_category = 'Fast'
    elif tempo >= 90:
        tempo_category = 'Moderate'
    elif tempo >= 60:
        tempo_category = 'Slow'
    else:
        tempo_category = 'Very Slow'
    
    # Categorize Acousticness Level
    if acousticness >= 0.8:
        acousticness_level = 'Very High'
    elif acousticness >= 0.6:
        acousticness_level = 'High'
    elif acousticness >= 0.4:
        acousticness_level = 'Medium'
    elif acousticness >= 0.2:
        acousticness_level = 'Low'
    else:
        acousticness_level = 'Very Low'
    
    return (energy_level, danceability_level, valence_level, tempo_category, acousticness_level)

# ========================================
# PHẦN 2: SCHEMA CREATION
# ========================================

def create_tables(cur):
    """
    Tạo schema với 11 bảng: 6 Dimensions + 5 Facts
    
    DIMENSION TABLES (6):
    1. dim_song - Thông tin bài hát
    2. dim_artist - Thông tin nghệ sĩ
    3. dim_album - Thông tin album
    4. dim_date - Thông tin thời gian
    5. dim_country - Thông tin quốc gia
    6. dim_audio_features - Phân loại đặc điểm âm nhạc
    
    FACT TABLES (5):
    1. fact_song_daily - Hiệu suất bài hát hàng ngày
    2. fact_artist_stats - Thống kê nghệ sĩ
    3. fact_chart_position - Vị trí và di chuyển BXH
    4. fact_audio_analysis - Phân tích đặc điểm âm nhạc
    5. fact_streaming_metrics - Metrics streaming và engagement
    """
    
    commands = (
        # Drop all tables
        "DROP TABLE IF EXISTS fact_streaming_metrics CASCADE;",
        "DROP TABLE IF EXISTS fact_audio_analysis CASCADE;",
        "DROP TABLE IF EXISTS fact_chart_position CASCADE;",
        "DROP TABLE IF EXISTS fact_artist_stats CASCADE;",
        "DROP TABLE IF EXISTS fact_song_daily CASCADE;",
        "DROP TABLE IF EXISTS dim_audio_features CASCADE;",
        "DROP TABLE IF EXISTS dim_country CASCADE;",
        "DROP TABLE IF EXISTS dim_date CASCADE;",
        "DROP TABLE IF EXISTS dim_album CASCADE;",
        "DROP TABLE IF EXISTS dim_artist CASCADE;",
        "DROP TABLE IF EXISTS dim_song CASCADE;",
        
        # ==================== DIMENSION TABLES ====================
        
        # DIM 1: Song Information
        """
        CREATE TABLE dim_song (
            song_id SERIAL PRIMARY KEY,
            spotify_id VARCHAR(255) UNIQUE NOT NULL,
            song_name TEXT NOT NULL,
            duration_ms INTEGER,
            is_explicit BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        COMMENT ON TABLE dim_song IS 'Dimension: Thông tin cơ bản về bài hát';
        """,
        
        # DIM 2: Artist Information  
        """
        CREATE TABLE dim_artist (
            artist_id SERIAL PRIMARY KEY,
            artist_name TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        COMMENT ON TABLE dim_artist IS 'Dimension: Thông tin nghệ sĩ';
        """,
        
        # DIM 3: Album Information
        """
        CREATE TABLE dim_album (
            album_id SERIAL PRIMARY KEY,
            album_name TEXT NOT NULL,
            release_date DATE,
            release_year INTEGER,
            release_month INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(album_name, release_date)
        );
        COMMENT ON TABLE dim_album IS 'Dimension: Thông tin album';
        """,
        
        # DIM 4: Date Dimension (Time Intelligence)
        """
        CREATE TABLE dim_date (
            date_id SERIAL PRIMARY KEY,
            full_date DATE UNIQUE NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            month_name VARCHAR(20),
            day INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            day_name VARCHAR(20),
            week_of_year INTEGER,
            is_weekend BOOLEAN,
            season VARCHAR(20)
        );
        COMMENT ON TABLE dim_date IS 'Dimension: Thông tin thời gian với date intelligence';
        """,
        
        # DIM 5: Country/Region Information
        """
        CREATE TABLE dim_country (
            country_id SERIAL PRIMARY KEY,
            country_code VARCHAR(10) UNIQUE NOT NULL,
            country_name VARCHAR(255) NOT NULL
        );
        COMMENT ON TABLE dim_country IS 'Dimension: Thông tin quốc gia';
        """,
        
        # DIM 6: Audio Features (Audio Characteristics Categorization)
        """
        CREATE TABLE dim_audio_features (
            features_id SERIAL PRIMARY KEY,
            energy_level VARCHAR(20) NOT NULL,
            danceability_level VARCHAR(20) NOT NULL,
            valence_level VARCHAR(20) NOT NULL,
            tempo_category VARCHAR(20) NOT NULL,
            acousticness_level VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(energy_level, danceability_level, valence_level, tempo_category, acousticness_level)
        );
        COMMENT ON TABLE dim_audio_features IS 'Dimension: Phân loại đặc điểm âm nhạc dựa trên audio features';
        """,
        
        # ==================== FACT TABLES ====================
        
        # FACT 1: Song Daily Performance
        """
        CREATE TABLE fact_song_daily (
            fact_id SERIAL PRIMARY KEY,
            song_id INTEGER NOT NULL REFERENCES dim_song(song_id),
            date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
            country_id INTEGER REFERENCES dim_country(country_id),
            album_id INTEGER REFERENCES dim_album(album_id),
            
            -- Performance Metrics
            daily_rank INTEGER,
            popularity_score INTEGER CHECK (popularity_score BETWEEN 0 AND 100),
            
            -- Calculated Metrics
            rank_points NUMERIC(10,2),
            performance_index NUMERIC(10,2),
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(song_id, date_id, country_id)
        );
        COMMENT ON TABLE fact_song_daily IS 'Fact: Hiệu suất hàng ngày của bài hát';
        CREATE INDEX idx_fact_song_daily_song ON fact_song_daily(song_id);
        CREATE INDEX idx_fact_song_daily_date ON fact_song_daily(date_id);
        CREATE INDEX idx_fact_song_daily_country ON fact_song_daily(country_id);
        """,
        
        # FACT 2: Artist Statistics
        """
        CREATE TABLE fact_artist_stats (
            fact_id SERIAL PRIMARY KEY,
            artist_id INTEGER NOT NULL REFERENCES dim_artist(artist_id),
            song_id INTEGER NOT NULL REFERENCES dim_song(song_id),
            date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
            country_id INTEGER REFERENCES dim_country(country_id),
            
            -- Artist Metrics
            song_rank INTEGER,
            song_popularity INTEGER,
            artist_position INTEGER,
            
            -- Calculated Metrics
            artist_score NUMERIC(10,2),
            contribution_weight NUMERIC(5,2),
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(artist_id, song_id, date_id, country_id)
        );
        COMMENT ON TABLE fact_artist_stats IS 'Fact: Thống kê hiệu suất nghệ sĩ';
        CREATE INDEX idx_fact_artist_stats_artist ON fact_artist_stats(artist_id);
        CREATE INDEX idx_fact_artist_stats_song ON fact_artist_stats(song_id);
        CREATE INDEX idx_fact_artist_stats_date ON fact_artist_stats(date_id);
        """,
        
        # FACT 3: Chart Position & Movement
        """
        CREATE TABLE fact_chart_position (
            fact_id SERIAL PRIMARY KEY,
            song_id INTEGER NOT NULL REFERENCES dim_song(song_id),
            date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
            country_id INTEGER REFERENCES dim_country(country_id),
            
            -- Chart Metrics
            current_rank INTEGER,
            previous_rank INTEGER,
            daily_movement INTEGER,
            weekly_movement INTEGER,
            
            -- Movement Indicators
            is_rising BOOLEAN,
            is_falling BOOLEAN,
            movement_magnitude INTEGER,
            trend_strength NUMERIC(8,2),
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(song_id, date_id, country_id)
        );
        COMMENT ON TABLE fact_chart_position IS 'Fact: Vị trí và di chuyển trên bảng xếp hạng';
        CREATE INDEX idx_fact_chart_position_song ON fact_chart_position(song_id);
        CREATE INDEX idx_fact_chart_position_date ON fact_chart_position(date_id);
        CREATE INDEX idx_fact_chart_position_rising ON fact_chart_position(is_rising);
        """,
        
        # FACT 4: Audio Analysis
        """
        CREATE TABLE fact_audio_analysis (
            fact_id SERIAL PRIMARY KEY,
            song_id INTEGER NOT NULL UNIQUE REFERENCES dim_song(song_id),
            features_id INTEGER REFERENCES dim_audio_features(features_id),
            
            -- Audio Features (Spotify API)
            danceability REAL CHECK (danceability BETWEEN 0 AND 1),
            energy REAL CHECK (energy BETWEEN 0 AND 1),
            key_signature INTEGER CHECK (key_signature BETWEEN 0 AND 11),
            loudness REAL,
            mode INTEGER CHECK (mode IN (0, 1)),
            speechiness REAL CHECK (speechiness BETWEEN 0 AND 1),
            acousticness REAL CHECK (acousticness BETWEEN 0 AND 1),
            instrumentalness REAL CHECK (instrumentalness BETWEEN 0 AND 1),
            liveness REAL CHECK (liveness BETWEEN 0 AND 1),
            valence REAL CHECK (valence BETWEEN 0 AND 1),
            tempo REAL CHECK (tempo > 0),
            time_signature INTEGER,
            
            -- Derived Metrics
            energy_dance_score REAL,
            mood_score REAL,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        COMMENT ON TABLE fact_audio_analysis IS 'Fact: Phân tích đặc điểm âm nhạc của bài hát';
        CREATE INDEX idx_fact_audio_analysis_song ON fact_audio_analysis(song_id);
        CREATE INDEX idx_fact_audio_analysis_features ON fact_audio_analysis(features_id);
        """,
        
        # FACT 5: Streaming Metrics
        """
        CREATE TABLE fact_streaming_metrics (
            fact_id SERIAL PRIMARY KEY,
            song_id INTEGER NOT NULL REFERENCES dim_song(song_id),
            date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
            country_id INTEGER REFERENCES dim_country(country_id),
            
            -- Streaming Metrics (Estimated)
            estimated_streams INTEGER,
            estimated_listeners INTEGER,
            
            -- Engagement Metrics
            avg_completion_rate NUMERIC(5,2),
            engagement_score NUMERIC(10,2),
            viral_coefficient NUMERIC(8,2),
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(song_id, date_id, country_id)
        );
        COMMENT ON TABLE fact_streaming_metrics IS 'Fact: Metrics về streaming và engagement';
        CREATE INDEX idx_fact_streaming_song ON fact_streaming_metrics(song_id);
        CREATE INDEX idx_fact_streaming_date ON fact_streaming_metrics(date_id);
        """,
    )
    
    for command in commands:
        cur.execute(command)
    
    print("✅ Schema created successfully!")
    print("   📊 6 Dimension Tables")
    print("   📈 5 Fact Tables")
    print("   📝 Total: 11 Tables")

# ========================================
# PHẦN 3: ETL PROCESS
# ========================================

def extract_data(csv_file, chunk_size=10000):
    """EXTRACT: Đọc dữ liệu từ CSV"""
    print(f"\n📥 EXTRACT: Đọc dữ liệu từ {csv_file}")
    return pd.read_csv(csv_file, chunksize=chunk_size, iterator=True)

def transform_data(chunk):
    """TRANSFORM: Làm sạch và biến đổi dữ liệu"""
    print(f"\n🔄 TRANSFORM: Đang xử lý {len(chunk)} dòng dữ liệu")
    
    df = chunk.copy()
    
    # Làm sạch text
    df['name'] = df['name'].apply(clean_text)
    df['artists'] = df['artists'].apply(clean_text)
    df['album_name'] = df['album_name'].apply(clean_text)
    df['country'] = df['country'].apply(lambda x: clean_text(x) if x and x != '' else 'GLOBAL')
    
    # Làm sạch boolean
    df['is_explicit'] = df['is_explicit'].apply(clean_boolean)
    
    # Làm sạch numeric
    df['duration_ms'] = df['duration_ms'].apply(lambda x: clean_numeric(x, min_val=0, default=180000))
    df['daily_rank'] = df['daily_rank'].apply(lambda x: clean_numeric(x, min_val=1, default=None))
    df['popularity'] = df['popularity'].apply(lambda x: clean_numeric(x, min_val=0, max_val=100, default=50))
    
    # Làm sạch audio features
    df['danceability'] = df['danceability'].apply(lambda x: clean_numeric(x, min_val=0, max_val=1, default=0.5))
    df['energy'] = df['energy'].apply(lambda x: clean_numeric(x, min_val=0, max_val=1, default=0.5))
    df['speechiness'] = df['speechiness'].apply(lambda x: clean_numeric(x, min_val=0, max_val=1, default=0.05))
    df['acousticness'] = df['acousticness'].apply(lambda x: clean_numeric(x, min_val=0, max_val=1, default=0.5))
    df['instrumentalness'] = df['instrumentalness'].apply(lambda x: clean_numeric(x, min_val=0, max_val=1, default=0))
    df['liveness'] = df['liveness'].apply(lambda x: clean_numeric(x, min_val=0, max_val=1, default=0.1))
    df['valence'] = df['valence'].apply(lambda x: clean_numeric(x, min_val=0, max_val=1, default=0.5))
    df['tempo'] = df['tempo'].apply(lambda x: clean_numeric(x, min_val=30, max_val=250, default=120))
    df['loudness'] = df['loudness'].apply(lambda x: clean_numeric(x, min_val=-60, max_val=0, default=-7))
    df['key'] = df['key'].apply(lambda x: clean_numeric(x, min_val=0, max_val=11, default=0))
    df['mode'] = df['mode'].apply(lambda x: 1 if clean_numeric(x, default=1) >= 0.5 else 0)
    df['time_signature'] = df['time_signature'].apply(lambda x: clean_numeric(x, min_val=3, max_val=7, default=4))
    
    # Làm sạch dates
    df['snapshot_date'] = df['snapshot_date'].apply(clean_date)
    df['album_release_date'] = df['album_release_date'].apply(clean_date)
    
    # Tính toán mood category
    df['mood_category'] = df.apply(lambda row: categorize_mood(row['valence'], row['energy']), axis=1)
    
    # Tính toán audio features categorization (tuple of 5 levels)
    df['audio_features_tuple'] = df.apply(
        lambda row: categorize_audio_features(
            row['energy'], 
            row['danceability'], 
            row['valence'],
            row['acousticness'],
            row['tempo']
        ), 
        axis=1
    )
    
    # Loại bỏ dòng có giá trị critical bị thiếu
    df = df.dropna(subset=['spotify_id', 'name', 'snapshot_date'])
    
    # Loại bỏ duplicate
    df = df.drop_duplicates(subset=['spotify_id', 'artists', 'snapshot_date', 'country'])
    
    print(f"✅ TRANSFORM: Hoàn thành. Còn lại {len(df)} dòng hợp lệ")
    print(f"   - Categorized audio features for {len(df)} songs")
    return df

def load_dimensions(df, cur):
    """LOAD: Nạp dữ liệu vào các bảng dimension"""
    print("\n📤 LOAD DIMENSIONS:")
    
    # 1. Load dim_artist
    all_artists = set()
    for artists_str in df['artists'].dropna():
        artists = extract_and_clean_artists(artists_str)
        all_artists.update(artists)
    
    if all_artists:
        # Lấy first created_at từ snapshot_date
        first_date = df['snapshot_date'].min()
        artist_values = [(artist, first_date) for artist in all_artists if artist]
        extras.execute_values(
            cur,
            "INSERT INTO dim_artist (artist_name, created_at) VALUES %s ON CONFLICT (artist_name) DO NOTHING",
            artist_values
        )
        print(f"   ✓ dim_artist: {len(artist_values)} records")
    
    # 2. Load dim_album
    first_date = df['snapshot_date'].min()
    albums = df[['album_name', 'album_release_date']].dropna(subset=['album_name']).drop_duplicates()
    if not albums.empty:
        album_values = []
        for _, row in albums.iterrows():
            release_date = row['album_release_date']
            year = release_date.year if release_date else None
            month = release_date.month if release_date else None
            album_values.append((row['album_name'], release_date, year, month, first_date))
        
        extras.execute_values(
            cur,
            "INSERT INTO dim_album (album_name, release_date, release_year, release_month, created_at) VALUES %s ON CONFLICT (album_name, release_date) DO NOTHING",
            album_values
        )
        print(f"   ✓ dim_album: {len(album_values)} records")
    
    # 3. Load dim_date
    dates = df['snapshot_date'].dropna().drop_duplicates()
    if not dates.empty:
        date_values = []
        month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June', 
                      'July', 'August', 'September', 'October', 'November', 'December']
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        for date in dates:
            month = date.month
            season = 'Winter' if month in [12, 1, 2] else 'Spring' if month in [3, 4, 5] else 'Summer' if month in [6, 7, 8] else 'Fall'
            
            date_values.append((
                date,
                date.year,
                (month - 1) // 3 + 1,
                month,
                month_names[month],
                date.day,
                date.weekday(),
                day_names[date.weekday()],
                date.isocalendar()[1],
                date.weekday() >= 5,
                season
            ))
        
        extras.execute_values(
            cur,
            "INSERT INTO dim_date (full_date, year, quarter, month, month_name, day, day_of_week, day_name, week_of_year, is_weekend, season) VALUES %s ON CONFLICT (full_date) DO NOTHING",
            date_values
        )
        print(f"   ✓ dim_date: {len(date_values)} records")
    
    # 4. Load dim_country
    countries = df['country'].dropna().drop_duplicates()
    if not countries.empty:
        country_values = [(country, country) for country in countries if country and country != '']
        if country_values:
            extras.execute_values(
                cur,
                "INSERT INTO dim_country (country_code, country_name) VALUES %s ON CONFLICT (country_code) DO NOTHING",
                country_values
            )
            print(f"   ✓ dim_country: {len(country_values)} records")
    
    # Thêm 1 country mặc định cho GLOBAL (xử lý NULL)
    cur.execute("INSERT INTO dim_country (country_code, country_name) VALUES (%s, %s) ON CONFLICT (country_code) DO NOTHING", ('GLOBAL', 'Global'))
    print(f"   ✓ dim_country: Added default 'GLOBAL' country")
    
    # 1. Load dim_song
    first_date = df['snapshot_date'].min()
    songs = df[['spotify_id', 'name', 'is_explicit', 'duration_ms']].drop_duplicates(subset=['spotify_id'])
    if not songs.empty:
        song_values = [
            (row['spotify_id'], row['name'], row['is_explicit'], int(row['duration_ms']), first_date)
            for _, row in songs.iterrows()
        ]
        extras.execute_values(
            cur,
            "INSERT INTO dim_song (spotify_id, song_name, is_explicit, duration_ms, created_at) VALUES %s ON CONFLICT (spotify_id) DO NOTHING",
            song_values
        )
        print(f"   ✓ dim_song: {len(song_values)} records")
    
    # 6. Load dim_audio_features - Audio Feature Combinations
    # Thu thập tất cả các audio features tuple từ dataframe
    audio_features_set = set(df['audio_features_tuple'].unique())
    
    if audio_features_set:
        first_date = df['snapshot_date'].min()
        features_values = [
            (energy_lvl, dance_lvl, val_lvl, tempo_cat, acoustic_lvl, first_date)
            for energy_lvl, dance_lvl, val_lvl, tempo_cat, acoustic_lvl in audio_features_set
        ]
        extras.execute_values(
            cur,
            """INSERT INTO dim_audio_features 
               (energy_level, danceability_level, valence_level, tempo_category, acousticness_level, created_at) 
               VALUES %s 
               ON CONFLICT (energy_level, danceability_level, valence_level, tempo_category, acousticness_level) 
               DO NOTHING""",
            features_values
        )
        print(f"   ✓ dim_audio_features: {len(features_values)} feature combinations")

def load_facts(df, cur):
    """LOAD: Nạp dữ liệu vào các bảng fact"""
    print("\n📤 LOAD FACTS:")
    
    # Lấy dimension keys
    cur.execute("SELECT spotify_id, song_id FROM dim_song")
    song_map = dict(cur.fetchall())
    
    cur.execute("SELECT artist_name, artist_id FROM dim_artist")
    artist_map = dict(cur.fetchall())
    
    cur.execute("SELECT album_name, release_date, album_id FROM dim_album")
    album_map = {(name, date): id for name, date, id in cur.fetchall()}
    
    cur.execute("SELECT full_date, date_id FROM dim_date")
    date_map = dict(cur.fetchall())
    
    cur.execute("SELECT country_code, country_id FROM dim_country")
    country_map = dict(cur.fetchall())
    
    # QUAN TRỌNG: Lấy features_id từ dim_audio_features
    cur.execute("""SELECT energy_level, danceability_level, valence_level, 
                   tempo_category, acousticness_level, features_id 
                   FROM dim_audio_features""")
    features_map = {(e, d, v, t, a): fid for e, d, v, t, a, fid in cur.fetchall()}
    
    # Chuẩn bị dữ liệu cho các fact tables
    fact_song_daily = []
    fact_artist_stats = []
    fact_chart_position = []
    fact_audio_analysis = []
    fact_streaming_metrics = []
    
    processed_audio = set()
    
    for _, row in df.iterrows():
        song_id = song_map.get(row['spotify_id'])
        date_id = date_map.get(row['snapshot_date'])
        
        # XỬ LÝ NULL COUNTRY: Nếu country null/empty thì dùng 'GLOBAL'
        country_val = row['country'] if pd.notna(row['country']) and row['country'] != '' else 'GLOBAL'
        country_id = country_map.get(country_val)
        if not country_id:  # Fallback nếu vẫn không tìm thấy
            country_id = country_map.get('GLOBAL')
        
        album_id = album_map.get((row['album_name'], row['album_release_date']))
        
        if not song_id or not date_id:
            continue
        
        # Tính toán các metrics - XỬ LÝ NULL VALUES
        rank = int(row['daily_rank']) if pd.notna(row['daily_rank']) else 100
        popularity = int(row['popularity']) if pd.notna(row['popularity']) else 50
        rank_points = (101 - rank)
        performance_index = (rank_points + popularity) / 2
        
        # FACT 1: Song Daily Performance
        fact_song_daily.append((
            song_id, date_id, country_id, album_id,
            rank, popularity, rank_points, performance_index, row['snapshot_date']
        ))
        
        # FACT 3: Chart Position - XỬ LÝ NULL MOVEMENTS
        daily_mov = int(row['daily_movement']) if pd.notna(row['daily_movement']) else 0
        weekly_mov = int(row['weekly_movement']) if pd.notna(row['weekly_movement']) else 0
        is_rising = daily_mov > 0 or weekly_mov > 0
        is_falling = daily_mov < 0 or weekly_mov < 0
        movement_mag = abs(daily_mov) + abs(weekly_mov)
        trend_strength = min(10.0, movement_mag / 10.0)  # Cap at 10.0
        
        # Calculate previous_rank từ current rank và daily_movement
        previous_rank = rank - daily_mov if rank and daily_mov else rank
        if not previous_rank or previous_rank < 1:
            previous_rank = 100  # Default to 100 if invalid
        
        fact_chart_position.append((
            song_id, date_id, country_id,
            rank, int(previous_rank), daily_mov, weekly_mov,
            is_rising, is_falling, movement_mag, trend_strength, row['snapshot_date']
        ))
        
        # FACT 4: Audio Analysis (chỉ 1 lần mỗi bài) - XỬ LÝ NULL AUDIO FEATURES
        if song_id not in processed_audio:
            energy = float(row['energy']) if pd.notna(row['energy']) else 0.5
            danceability = float(row['danceability']) if pd.notna(row['danceability']) else 0.5
            valence = float(row['valence']) if pd.notna(row['valence']) else 0.5
            
            energy_dance = (energy + danceability) / 2
            mood_score = (valence * 0.6 + energy * 0.4)
            
            # LẤY FEATURES_ID TỰ ĐỘNG TỪ DIM_AUDIO_FEATURES
            audio_features_tuple = row.get('audio_features_tuple', None)
            features_id = features_map.get(audio_features_tuple) if audio_features_tuple else None
            
            fact_audio_analysis.append((
                song_id, features_id,
                danceability, energy,
                int(row['key']) if pd.notna(row['key']) else 0,
                float(row['loudness']) if pd.notna(row['loudness']) else -7.0,
                int(row['mode']) if pd.notna(row['mode']) else 1,
                float(row['speechiness']) if pd.notna(row['speechiness']) else 0.05,
                float(row['acousticness']) if pd.notna(row['acousticness']) else 0.5,
                float(row['instrumentalness']) if pd.notna(row['instrumentalness']) else 0.0,
                float(row['liveness']) if pd.notna(row['liveness']) else 0.1,
                valence,
                float(row['tempo']) if pd.notna(row['tempo']) else 120.0,
                int(row['time_signature']) if pd.notna(row['time_signature']) else 4,
                energy_dance, mood_score, row['snapshot_date']
            ))
            processed_audio.add(song_id)
        
        # FACT 5: Streaming Metrics (ước tính) - XỬ LÝ NULL STREAMS
        estimated_streams = int((101 - rank) * 10000)
        estimated_listeners = max(0, int(estimated_streams * 0.6))
        engagement = min(100.0, popularity * 1.2)  # Cap at 100
        viral_coef = min(1.0, movement_mag / 100.0)  # Cap at 1.0
        
        fact_streaming_metrics.append((
            song_id, date_id, country_id,
            estimated_streams, estimated_listeners,
            85.0, engagement, viral_coef, row['snapshot_date']
        ))
        
        # FACT 2: Artist Stats - XỬ LÝ ARTISTS KHÔNG CÓ
        artists = extract_and_clean_artists(row['artists'])
        if not artists:  # Nếu không có artist thì skip
            continue
            
        for idx, artist_name in enumerate(artists):
            artist_id = artist_map.get(artist_name)
            if artist_id:
                artist_score = performance_index * (1.0 if idx == 0 else 0.7)
                contribution = 1.0 if idx == 0 else 0.5
                
                fact_artist_stats.append((
                    artist_id, song_id, date_id, country_id,
                    rank, popularity, idx + 1,
                    artist_score, contribution, row['snapshot_date']
                ))
    
    # Insert vào database
    if fact_song_daily:
        extras.execute_values(
            cur,
            """INSERT INTO fact_song_daily 
               (song_id, date_id, country_id, album_id, daily_rank, popularity_score, 
                rank_points, performance_index, created_at) 
               VALUES %s ON CONFLICT (song_id, date_id, country_id) DO NOTHING""",
            fact_song_daily
        )
        print(f"   ✓ fact_song_daily: {len(fact_song_daily)} records")
    
    if fact_artist_stats:
        extras.execute_values(
            cur,
            """INSERT INTO fact_artist_stats 
               (artist_id, song_id, date_id, country_id, song_rank, song_popularity, 
                artist_position, artist_score, contribution_weight, created_at) 
               VALUES %s ON CONFLICT (artist_id, song_id, date_id, country_id) DO NOTHING""",
            fact_artist_stats
        )
        print(f"   ✓ fact_artist_stats: {len(fact_artist_stats)} records")
    
    if fact_chart_position:
        extras.execute_values(
            cur,
            """INSERT INTO fact_chart_position 
               (song_id, date_id, country_id, current_rank, previous_rank, 
                daily_movement, weekly_movement, is_rising, is_falling, 
                movement_magnitude, trend_strength, created_at) 
               VALUES %s ON CONFLICT (song_id, date_id, country_id) DO NOTHING""",
            fact_chart_position
        )
        print(f"   ✓ fact_chart_position: {len(fact_chart_position)} records")
    
    if fact_audio_analysis:
        extras.execute_values(
            cur,
            """INSERT INTO fact_audio_analysis 
               (song_id, features_id, danceability, energy, key_signature, loudness, mode,
                speechiness, acousticness, instrumentalness, liveness, valence, tempo,
                time_signature, energy_dance_score, mood_score, created_at) 
               VALUES %s ON CONFLICT (song_id) DO NOTHING""",
            fact_audio_analysis
        )
        print(f"   ✓ fact_audio_analysis: {len(fact_audio_analysis)} records")
    
    if fact_streaming_metrics:
        extras.execute_values(
            cur,
            """INSERT INTO fact_streaming_metrics 
               (song_id, date_id, country_id, estimated_streams, estimated_listeners,
                avg_completion_rate, engagement_score, viral_coefficient, created_at) 
               VALUES %s ON CONFLICT (song_id, date_id, country_id) DO NOTHING""",
            fact_streaming_metrics
        )
        print(f"   ✓ fact_streaming_metrics: {len(fact_streaming_metrics)} records")

# ========================================
# PHẦN 4: MAIN PIPELINE
# ========================================

def main():
    """Main ETL Pipeline"""
    print("\n" + "="*80)
    print("  🎵 SPOTIFY DATA WAREHOUSE - STUDENT PROJECT VERSION")
    print("="*80)
    print("  📊 Schema: Constellation (Galaxy) Schema")
    print("  📋 Tables: 6 Dimensions + 5 Facts = 11 Tables")
    print("  👥 Phù hợp: Đồ án nhóm 4-5 sinh viên")
    print("="*80 + "\n")
    
    try:
        # Kết nối database
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        
        # Tạo schema
        print("📋 BƯỚC 1: TẠO SCHEMA")
        print("-" * 80)
        create_tables(cur)
        conn.commit()
        
        # ETL Process
        csv_file = 'universal_top_spotify_songs.csv'
        chunk_size = 10000
        
        print("\n📊 BƯỚC 2: ETL PROCESS")
        print("="*80)
        
        chunk_iterator = extract_data(csv_file, chunk_size)
        
        chunk_count = 0
        for i, chunk in enumerate(chunk_iterator):
            chunk_count += 1
            print(f"\n{'─'*80}")
            print(f"📦 CHUNK {chunk_count}")
            print(f"{'─'*80}")
            
            # Transform
            cleaned_chunk = transform_data(chunk)
            
            if len(cleaned_chunk) == 0:
                print("⚠️  Không có dữ liệu hợp lệ, bỏ qua chunk này")
                continue
            
            # Load
            load_dimensions(cleaned_chunk, cur)
            load_facts(cleaned_chunk, cur)
            
            conn.commit()
            print(f"\n✅ Chunk {chunk_count} hoàn thành và đã commit")
        
        # Thống kê cuối cùng
        print("\n" + "="*80)
        print("✅ ETL PIPELINE HOÀN THÀNH")
        print("="*80)
        print("\n📊 THỐNG KÊ KHO DỮ LIỆU:")
        print("-" * 80)
        
        tables = [
            ('DIMENSIONS', [
                'dim_song', 'dim_artist', 'dim_album', 
                'dim_date', 'dim_country', 'dim_audio_features'
            ]),
            ('FACTS', [
                'fact_song_daily', 'fact_artist_stats', 'fact_chart_position',
                'fact_audio_analysis', 'fact_streaming_metrics'
            ])
        ]
        
        for category, table_list in tables:
            print(f"\n{category}:")
            for table in table_list:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"  {table:.<45} {count:>15,} rows")
        
        print("-" * 80)
        
        # Tổng số bảng
        print(f"\n📋 Tổng số bảng: 11 (6 Dimensions + 5 Facts)")
        print(f"✅ Kho dữ liệu đã sẵn sàng để phân tích!\n")
        
        cur.close()
        conn.close()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"\n❌ LỖI: {error}")
        raise

if __name__ == '__main__':
    main()
