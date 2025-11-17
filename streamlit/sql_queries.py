# -*- coding: utf-8 -*-
"""
SQL Queries cho phân tích xu hướng âm nhạc và độ phổ biến nghệ sĩ toàn cầu từ Spotify
Cấu trúc database: dim_song, dim_artist không có quan hệ trực tiếp
Cần join qua fact_artist_stats để lấy thông tin nghệ sĩ
"""

# ============================================
# 1. XU HƯỚNG ÂM NHẠC TOÀN CẦU
# ============================================

# 1.1 Top 20 bài hát phổ biến nhất toàn cầu
QUERY_TOP_SONGS_GLOBAL = """
SELECT 
    s.song_name,
    STRING_AGG(DISTINCT a.artist_name, ', ') as artist_name,
    COUNT(DISTINCT c.country_name) as num_countries,
    AVG(fsd.popularity_score) as avg_popularity,
    AVG(fsd.daily_rank) as avg_rank,
    MAX(fsd.popularity_score) as max_popularity
FROM fact_song_daily fsd
JOIN dim_song s ON fsd.song_id = s.song_id
LEFT JOIN fact_artist_stats fas ON fsd.song_id = fas.song_id AND fsd.date_id = fas.date_id
LEFT JOIN dim_artist a ON fas.artist_id = a.artist_id
JOIN dim_country c ON fsd.country_id = c.country_id
GROUP BY s.song_name
ORDER BY avg_popularity DESC, num_countries DESC
LIMIT 20;
"""

# 1.2 Xu hướng bài hát theo thời gian (30 ngày gần nhất)
QUERY_TRENDING_SONGS = """
WITH recent_data AS (
    SELECT 
        s.song_name,
        STRING_AGG(DISTINCT a.artist_name, ', ') as artist_name,
        d.full_date as date,
        AVG(fsd.daily_rank) as avg_rank,
        AVG(fsd.popularity_score) as avg_popularity
    FROM fact_song_daily fsd
    JOIN dim_song s ON fsd.song_id = s.song_id
    LEFT JOIN fact_artist_stats fas ON fsd.song_id = fas.song_id AND fsd.date_id = fas.date_id
    LEFT JOIN dim_artist a ON fas.artist_id = a.artist_id
    JOIN dim_date d ON fsd.date_id = d.date_id
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY s.song_name, d.full_date
)
SELECT 
    song_name,
    artist_name,
    date,
    avg_rank,
    avg_popularity,
    LAG(avg_rank) OVER (PARTITION BY song_name ORDER BY date) as prev_rank,
    avg_rank - LAG(avg_rank) OVER (PARTITION BY song_name ORDER BY date) as rank_change
FROM recent_data
ORDER BY date DESC, avg_popularity DESC
LIMIT 50;
"""

# 1.3 Phân tích xu hướng theo đặc điểm âm thanh (thay thế genre)
QUERY_GENRE_TRENDS = """
WITH audio_categories AS (
    SELECT 
        CASE 
            WHEN faa.energy > 0.7 AND faa.danceability > 0.7 THEN 'High Energy Dance'
            WHEN faa.energy > 0.7 THEN 'High Energy'
            WHEN faa.danceability > 0.7 THEN 'Dance'
            WHEN faa.acousticness > 0.5 THEN 'Acoustic'
            WHEN faa.valence > 0.6 THEN 'Positive'
            WHEN faa.valence < 0.4 THEN 'Melancholic'
            ELSE 'Balanced'
        END as music_category,
        fsd.song_id,
        fsd.popularity_score,
        fsd.daily_rank,
        fsd.rank_points
    FROM fact_song_daily fsd
    JOIN fact_audio_analysis faa ON fsd.song_id = faa.song_id
)
SELECT 
    music_category,
    COUNT(DISTINCT song_id) as num_songs,
    AVG(popularity_score) as avg_popularity,
    AVG(daily_rank) as avg_rank,
    SUM(rank_points) as total_rank_points
FROM audio_categories
GROUP BY music_category
ORDER BY avg_popularity DESC
LIMIT 15;
"""

# 1.4 Đặc điểm âm thanh của bài hát trending
QUERY_AUDIO_FEATURES_TRENDING = """
WITH audio_levels AS (
    SELECT 
        CASE 
            WHEN faa.energy < 0.3 THEN 'Low Energy'
            WHEN faa.energy < 0.7 THEN 'Medium Energy'
            ELSE 'High Energy'
        END as energy_level,
        CASE 
            WHEN faa.danceability < 0.3 THEN 'Low Dance'
            WHEN faa.danceability < 0.7 THEN 'Medium Dance'
            ELSE 'High Dance'
        END as danceability_level,
        CASE 
            WHEN faa.valence < 0.3 THEN 'Sad'
            WHEN faa.valence < 0.7 THEN 'Neutral'
            ELSE 'Happy'
        END as valence_level,
        CASE 
            WHEN faa.valence > 0.6 AND faa.energy > 0.6 THEN 'Energetic'
            WHEN faa.valence > 0.6 THEN 'Happy'
            WHEN faa.valence < 0.4 AND faa.energy < 0.4 THEN 'Sad'
            ELSE 'Neutral'
        END as mood,
        fsd.popularity_score
    FROM fact_song_daily fsd
    JOIN fact_audio_analysis faa ON fsd.song_id = faa.song_id
    WHERE fsd.popularity_score >= 70
)
SELECT 
    energy_level,
    danceability_level,
    valence_level,
    mood,
    COUNT(*) as song_count,
    AVG(popularity_score) as avg_popularity
FROM audio_levels
GROUP BY energy_level, danceability_level, valence_level, mood
ORDER BY song_count DESC
LIMIT 20;
"""

# ============================================
# 2. ĐỘ PHỔ BIẾN NGHỆ SĨ TOÀN CẦU
# ============================================

# 2.1 Top 20 nghệ sĩ phổ biến nhất
QUERY_TOP_ARTISTS = """
SELECT 
    a.artist_name,
    COUNT(DISTINCT fas.song_id) as total_songs,
    COUNT(DISTINCT fas.country_id) as countries_present,
    AVG(fas.artist_score) as avg_artist_score,
    AVG(fas.song_popularity) as avg_popularity
FROM fact_artist_stats fas
JOIN dim_artist a ON fas.artist_id = a.artist_id
GROUP BY a.artist_name
ORDER BY avg_artist_score DESC, countries_present DESC
LIMIT 20;
"""

# 2.2 Nghệ sĩ có độ phủ sóng quốc tế cao nhất
QUERY_ARTISTS_GLOBAL_REACH = """
SELECT 
    a.artist_name,
    COUNT(DISTINCT c.country_name) as num_countries,
    COUNT(DISTINCT fas.song_id) as num_songs,
    AVG(fas.song_popularity) as avg_popularity
FROM fact_artist_stats fas
JOIN dim_artist a ON fas.artist_id = a.artist_id
JOIN dim_country c ON fas.country_id = c.country_id
GROUP BY a.artist_name
HAVING COUNT(DISTINCT c.country_name) >= 5
ORDER BY num_countries DESC, avg_popularity DESC
LIMIT 20;
"""

# 2.3 Nghệ sĩ đang trending (tăng trưởng nhanh)
QUERY_TRENDING_ARTISTS = """
WITH artist_metrics AS (
    SELECT 
        a.artist_name,
        d.week_of_year as week,
        d.year,
        AVG(fas.song_popularity) as avg_popularity,
        COUNT(DISTINCT fas.song_id) as num_songs_chart
    FROM fact_artist_stats fas
    JOIN dim_artist a ON fas.artist_id = a.artist_id
    JOIN dim_date d ON fas.date_id = d.date_id
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '60 days'
    GROUP BY a.artist_name, d.week_of_year, d.year
)
SELECT 
    artist_name,
    MAX(avg_popularity) as current_popularity,
    MIN(avg_popularity) as starting_popularity,
    MAX(avg_popularity) - MIN(avg_popularity) as popularity_growth,
    AVG(num_songs_chart) as avg_songs_in_chart
FROM artist_metrics
GROUP BY artist_name
HAVING MAX(avg_popularity) - MIN(avg_popularity) > 5
ORDER BY popularity_growth DESC
LIMIT 20;
"""

# 2.4 Phân tích nghệ sĩ theo số bài hát
QUERY_ARTIST_FOLLOWERS = """
SELECT 
    a.artist_name,
    COUNT(DISTINCT fas.song_id) as num_songs,
    AVG(fas.song_popularity) as avg_song_popularity,
    COUNT(DISTINCT fas.country_id) as countries_present,
    CASE 
        WHEN COUNT(DISTINCT fas.song_id) >= 50 THEN 'Prolific (50+)'
        WHEN COUNT(DISTINCT fas.song_id) >= 20 THEN 'Active (20-50)'
        WHEN COUNT(DISTINCT fas.song_id) >= 10 THEN 'Regular (10-20)'
        ELSE 'Emerging (<10)'
    END as artist_tier
FROM dim_artist a
LEFT JOIN fact_artist_stats fas ON a.artist_id = fas.artist_id
GROUP BY a.artist_name
ORDER BY num_songs DESC, avg_song_popularity DESC
LIMIT 50;
"""

# ============================================
# 3. PHÂN TÍCH THEO QUỐC GIA & KHU VỰC
# ============================================

# 3.1 Phổ biến nhất theo từng quốc gia
QUERY_TOP_SONGS_BY_COUNTRY = """
WITH ranked_songs AS (
    SELECT 
        c.country_name,
        s.song_name,
        STRING_AGG(DISTINCT a.artist_name, ', ') as artist_name,
        AVG(fsd.popularity_score) as avg_popularity,
        ROW_NUMBER() OVER (PARTITION BY c.country_name ORDER BY AVG(fsd.popularity_score) DESC) as rank
    FROM fact_song_daily fsd
    JOIN dim_country c ON fsd.country_id = c.country_id
    JOIN dim_song s ON fsd.song_id = s.song_id
    LEFT JOIN fact_artist_stats fas ON fsd.song_id = fas.song_id AND fsd.date_id = fas.date_id
    LEFT JOIN dim_artist a ON fas.artist_id = a.artist_id
    GROUP BY c.country_name, s.song_name
)
SELECT 
    country_name,
    song_name,
    artist_name,
    avg_popularity
FROM ranked_songs
WHERE rank = 1
ORDER BY country_name
LIMIT 50;
"""

# 3.2 So sánh độ phổ biến giữa các khu vực (top countries)
QUERY_POPULARITY_BY_CONTINENT = """
SELECT 
    c.country_name as region,
    COUNT(DISTINCT fsd.song_id) as unique_songs,
    COUNT(DISTINCT fas.artist_id) as unique_artists,
    AVG(fsd.popularity_score) as avg_popularity,
    MAX(fsd.popularity_score) as max_popularity
FROM fact_song_daily fsd
LEFT JOIN fact_artist_stats fas ON fsd.song_id = fas.song_id AND fsd.date_id = fas.date_id
JOIN dim_country c ON fsd.country_id = c.country_id
GROUP BY c.country_name
ORDER BY avg_popularity DESC
LIMIT 15;
"""

# 3.3 Thị trường âm nhạc lớn nhất (theo số lượng bài hát trong top charts)
QUERY_BIGGEST_MUSIC_MARKETS = """
SELECT 
    c.country_name,
    COUNT(DISTINCT fsd.song_id) as unique_songs_in_chart,
    COUNT(DISTINCT fas.artist_id) as unique_artists,
    AVG(fsd.popularity_score) as avg_popularity,
    SUM(fsd.rank_points) as total_rank_points
FROM fact_song_daily fsd
LEFT JOIN fact_artist_stats fas ON fsd.song_id = fas.song_id AND fsd.date_id = fas.date_id
JOIN dim_country c ON fsd.country_id = c.country_id
GROUP BY c.country_name
ORDER BY unique_songs_in_chart DESC, avg_popularity DESC
LIMIT 25;
"""

# 3.4 Sở thích âm nhạc theo khu vực (audio features)
QUERY_REGIONAL_MUSIC_PREFERENCES = """
WITH regional_audio AS (
    SELECT 
        c.country_name as region,
        CASE 
            WHEN faa.valence > 0.6 AND faa.energy > 0.6 THEN 'Happy & Energetic'
            WHEN faa.valence > 0.6 THEN 'Happy'
            WHEN faa.energy > 0.6 THEN 'Energetic'
            WHEN faa.valence < 0.4 THEN 'Melancholic'
            ELSE 'Neutral'
        END as mood,
        CASE 
            WHEN faa.energy < 0.3 THEN 'Low'
            WHEN faa.energy < 0.7 THEN 'Medium'
            ELSE 'High'
        END as energy_level,
        CASE 
            WHEN faa.danceability < 0.3 THEN 'Low'
            WHEN faa.danceability < 0.7 THEN 'Medium'
            ELSE 'High'
        END as danceability_level,
        fsd.popularity_score
    FROM fact_song_daily fsd
    JOIN dim_country c ON fsd.country_id = c.country_id
    JOIN fact_audio_analysis faa ON fsd.song_id = faa.song_id
)
SELECT 
    region,
    mood,
    energy_level,
    danceability_level,
    COUNT(*) as song_count,
    AVG(popularity_score) as avg_popularity
FROM regional_audio
GROUP BY region, mood, energy_level, danceability_level
ORDER BY region, song_count DESC
LIMIT 100;
"""

# ============================================
# 4. PHÂN TÍCH THEO THỜI GIAN
# ============================================

# 4.1 Xu hướng theo ngày trong tuần
QUERY_POPULARITY_BY_WEEKDAY = """
SELECT 
    d.day_of_week,
    d.day_name,
    COUNT(DISTINCT fsd.song_id) as num_songs,
    AVG(fsd.popularity_score) as avg_popularity,
    AVG(fsd.daily_rank) as avg_rank
FROM fact_song_daily fsd
JOIN dim_date d ON fsd.date_id = d.date_id
GROUP BY d.day_of_week, d.day_name
ORDER BY d.day_of_week;
"""

# 4.2 Xu hướng theo tháng
QUERY_POPULARITY_BY_MONTH = """
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT fsd.song_id) as num_songs,
    AVG(fsd.popularity_score) as avg_popularity,
    COUNT(DISTINCT fas.artist_id) as num_artists
FROM fact_song_daily fsd
LEFT JOIN fact_artist_stats fas ON fsd.song_id = fas.song_id AND fsd.date_id = fas.date_id
JOIN dim_date d ON fsd.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
"""

# 4.3 Bài hát giữ vị trí #1 lâu nhất
QUERY_LONGEST_NUMBER_ONE = """
SELECT 
    s.song_name,
    STRING_AGG(DISTINCT a.artist_name, ', ') as artist_name,
    c.country_name,
    COUNT(*) as days_at_number_one,
    MIN(d.full_date) as first_date,
    MAX(d.full_date) as last_date
FROM fact_song_daily fsd
JOIN dim_song s ON fsd.song_id = s.song_id
LEFT JOIN fact_artist_stats fas ON fsd.song_id = fas.song_id AND fsd.date_id = fas.date_id
LEFT JOIN dim_artist a ON fas.artist_id = a.artist_id
JOIN dim_country c ON fsd.country_id = c.country_id
JOIN dim_date d ON fsd.date_id = d.date_id
WHERE fsd.daily_rank = 1
GROUP BY s.song_name, c.country_name
ORDER BY days_at_number_one DESC
LIMIT 20;
"""

# ============================================
# 5. PHÂN TÍCH ALBUM VÀ BÀI HÁT
# ============================================

# 5.1 Album phổ biến nhất
QUERY_TOP_ALBUMS = """
SELECT 
    al.album_name,
    STRING_AGG(DISTINCT a.artist_name, ', ') as artist_name,
    al.release_year,
    COUNT(DISTINCT s.song_id) as num_songs,
    AVG(fsd.popularity_score) as avg_popularity,
    SUM(fsd.rank_points) as total_rank_points
FROM fact_song_daily fsd
JOIN dim_album al ON fsd.album_id = al.album_id
JOIN dim_song s ON fsd.song_id = s.song_id
LEFT JOIN fact_artist_stats fas ON fsd.song_id = fas.song_id AND fsd.date_id = fas.date_id
LEFT JOIN dim_artist a ON fas.artist_id = a.artist_id
WHERE al.album_name IS NOT NULL
GROUP BY al.album_name, al.release_year
ORDER BY avg_popularity DESC
LIMIT 20;
"""

# 5.2 Phân tích theo độ dài album
QUERY_ALBUM_TYPE_ANALYSIS = """
WITH album_sizes AS (
    SELECT 
        al.album_name,
        al.release_year,
        COUNT(DISTINCT s.song_id) as num_songs,
        AVG(fsd.popularity_score) as avg_popularity,
        AVG(s.duration_ms / 1000.0 / 60.0) as avg_duration_minutes,
        CASE 
            WHEN COUNT(DISTINCT s.song_id) >= 12 THEN 'Full Album (12+)'
            WHEN COUNT(DISTINCT s.song_id) >= 6 THEN 'EP (6-12)'
            WHEN COUNT(DISTINCT s.song_id) >= 2 THEN 'Single/Duo (2-5)'
            ELSE 'Single (1)'
        END as album_type
    FROM fact_song_daily fsd
    JOIN dim_album al ON fsd.album_id = al.album_id
    JOIN dim_song s ON fsd.song_id = s.song_id
    WHERE al.album_name IS NOT NULL
    GROUP BY al.album_name, al.release_year
)
SELECT 
    album_type,
    COUNT(DISTINCT album_name) as num_albums,
    SUM(num_songs) as total_songs,
    AVG(avg_popularity) as avg_popularity,
    AVG(avg_duration_minutes) as avg_duration_minutes
FROM album_sizes
GROUP BY album_type
ORDER BY avg_popularity DESC;
"""

# 5.3 Xu hướng phát hành album theo năm
QUERY_ALBUM_RELEASE_TRENDS = """
SELECT 
    al.release_year,
    COUNT(DISTINCT al.album_id) as num_albums,
    COUNT(DISTINCT s.song_id) as num_songs,
    AVG(fsd.popularity_score) as avg_popularity
FROM fact_song_daily fsd
JOIN dim_album al ON fsd.album_id = al.album_id
JOIN dim_song s ON fsd.song_id = s.song_id
WHERE al.release_year IS NOT NULL AND al.release_year >= 2000
GROUP BY al.release_year
ORDER BY al.release_year DESC;
"""

# ============================================
# 6. PHÂN TÍCH ĐẶC ĐIỂM ÂM THANH (AUDIO FEATURES)
# ============================================

# 6.1 Mối quan hệ giữa đặc điểm âm thanh và độ phổ biến
QUERY_AUDIO_FEATURES_POPULARITY = """
WITH audio_categories AS (
    SELECT 
        CASE 
            WHEN faa.energy < 0.3 THEN 'Low'
            WHEN faa.energy < 0.7 THEN 'Medium'
            ELSE 'High'
        END as energy_level,
        CASE 
            WHEN faa.danceability < 0.3 THEN 'Low'
            WHEN faa.danceability < 0.7 THEN 'Medium'
            ELSE 'High'
        END as danceability_level,
        CASE 
            WHEN faa.valence < 0.3 THEN 'Low'
            WHEN faa.valence < 0.7 THEN 'Medium'
            ELSE 'High'
        END as valence_level,
        CASE 
            WHEN faa.acousticness < 0.3 THEN 'Low'
            WHEN faa.acousticness < 0.7 THEN 'Medium'
            ELSE 'High'
        END as acousticness_level,
        CASE 
            WHEN faa.tempo < 90 THEN 'Slow'
            WHEN faa.tempo < 120 THEN 'Medium'
            WHEN faa.tempo < 150 THEN 'Fast'
            ELSE 'Very Fast'
        END as tempo_category,
        CASE 
            WHEN faa.valence > 0.6 AND faa.energy > 0.6 THEN 'Happy'
            WHEN faa.valence > 0.6 THEN 'Calm'
            WHEN faa.valence < 0.4 AND faa.energy > 0.6 THEN 'Energetic'
            WHEN faa.valence < 0.4 THEN 'Sad'
            ELSE 'Neutral'
        END as mood,
        fsd.popularity_score,
        s.duration_ms
    FROM fact_song_daily fsd
    JOIN fact_audio_analysis faa ON fsd.song_id = faa.song_id
    JOIN dim_song s ON fsd.song_id = s.song_id
)
SELECT 
    energy_level,
    danceability_level,
    valence_level,
    acousticness_level,
    tempo_category,
    mood,
    COUNT(*) as song_count,
    AVG(popularity_score) as avg_popularity,
    AVG(duration_ms / 1000.0 / 60.0) as avg_duration_minutes
FROM audio_categories
GROUP BY energy_level, danceability_level, valence_level, 
         acousticness_level, tempo_category, mood
ORDER BY avg_popularity DESC
LIMIT 30;
"""

# 6.2 Phân tích mood của bài hát phổ biến
QUERY_MOOD_ANALYSIS = """
WITH mood_categories AS (
    SELECT 
        CASE 
            WHEN faa.valence > 0.6 AND faa.energy > 0.6 THEN 'Happy'
            WHEN faa.valence > 0.6 AND faa.energy < 0.4 THEN 'Calm'
            WHEN faa.valence < 0.4 AND faa.energy > 0.6 THEN 'Energetic'
            WHEN faa.valence < 0.4 AND faa.energy < 0.4 THEN 'Sad'
            ELSE 'Neutral'
        END as mood,
        fsd.song_id,
        fsd.popularity_score,
        fsd.daily_rank,
        fsd.country_id
    FROM fact_song_daily fsd
    JOIN fact_audio_analysis faa ON fsd.song_id = faa.song_id
)
SELECT 
    mood,
    COUNT(DISTINCT song_id) as num_songs,
    AVG(popularity_score) as avg_popularity,
    AVG(daily_rank) as avg_rank,
    COUNT(DISTINCT country_id) as num_countries
FROM mood_categories
WHERE mood IS NOT NULL
GROUP BY mood
ORDER BY avg_popularity DESC;
"""

# 6.3 Bài hát explicit vs non-explicit
QUERY_EXPLICIT_ANALYSIS = """
SELECT 
    s.is_explicit,
    COUNT(DISTINCT fsd.song_id) as num_songs,
    AVG(fsd.popularity_score) as avg_popularity,
    AVG(fsd.daily_rank) as avg_rank,
    COUNT(DISTINCT fsd.country_id) as num_countries
FROM fact_song_daily fsd
JOIN dim_song s ON fsd.song_id = s.song_id
GROUP BY s.is_explicit
ORDER BY s.is_explicit DESC;
"""

# ============================================
# 7. DASHBOARD SUMMARY METRICS
# ============================================

# 7.1 Tổng quan thống kê
QUERY_SUMMARY_STATS = """
SELECT 
    COUNT(DISTINCT s.song_id) as total_songs,
    COUNT(DISTINCT a.artist_id) as total_artists,
    COUNT(DISTINCT c.country_name) as total_countries,
    COUNT(DISTINCT al.album_id) as total_albums,
    AVG(fsd.popularity_score) as avg_popularity,
    MAX(fsd.popularity_score) as max_popularity
FROM fact_song_daily fsd
LEFT JOIN dim_song s ON fsd.song_id = s.song_id
LEFT JOIN fact_artist_stats fas ON fsd.song_id = fas.song_id AND fsd.date_id = fas.date_id
LEFT JOIN dim_artist a ON fas.artist_id = a.artist_id
LEFT JOIN dim_country c ON fsd.country_id = c.country_id
LEFT JOIN dim_album al ON fsd.album_id = al.album_id;
"""

# 7.2 Thống kê bài hát theo duration
QUERY_DURATION_ANALYSIS = """
SELECT 
    CASE 
        WHEN s.duration_ms < 120000 THEN 'Very Short (<2min)'
        WHEN s.duration_ms < 180000 THEN 'Short (2-3min)'
        WHEN s.duration_ms < 240000 THEN 'Medium (3-4min)'
        WHEN s.duration_ms < 300000 THEN 'Long (4-5min)'
        ELSE 'Very Long (>5min)'
    END as duration_category,
    COUNT(DISTINCT fsd.song_id) as num_songs,
    AVG(fsd.popularity_score) as avg_popularity
FROM fact_song_daily fsd
JOIN dim_song s ON fsd.song_id = s.song_id
WHERE s.duration_ms > 0
GROUP BY duration_category
ORDER BY avg_popularity DESC;
"""

# Dictionary chứa tất cả queries để dễ truy cập
ALL_QUERIES = {
    # Xu hướng âm nhạc
    'top_songs_global': QUERY_TOP_SONGS_GLOBAL,
    'trending_songs': QUERY_TRENDING_SONGS,
    'genre_trends': QUERY_GENRE_TRENDS,
    'audio_features_trending': QUERY_AUDIO_FEATURES_TRENDING,
    
    # Độ phổ biến nghệ sĩ
    'top_artists': QUERY_TOP_ARTISTS,
    'artists_global_reach': QUERY_ARTISTS_GLOBAL_REACH,
    'trending_artists': QUERY_TRENDING_ARTISTS,
    'artist_followers': QUERY_ARTIST_FOLLOWERS,
    
    # Phân tích quốc gia
    'top_songs_by_country': QUERY_TOP_SONGS_BY_COUNTRY,
    'popularity_by_continent': QUERY_POPULARITY_BY_CONTINENT,
    'biggest_music_markets': QUERY_BIGGEST_MUSIC_MARKETS,
    'regional_music_preferences': QUERY_REGIONAL_MUSIC_PREFERENCES,
    
    # Phân tích thời gian
    'popularity_by_weekday': QUERY_POPULARITY_BY_WEEKDAY,
    'popularity_by_month': QUERY_POPULARITY_BY_MONTH,
    'longest_number_one': QUERY_LONGEST_NUMBER_ONE,
    
    # Album và bài hát
    'top_albums': QUERY_TOP_ALBUMS,
    'album_type_analysis': QUERY_ALBUM_TYPE_ANALYSIS,
    'album_release_trends': QUERY_ALBUM_RELEASE_TRENDS,
    
    # Đặc điểm âm thanh
    'audio_features_popularity': QUERY_AUDIO_FEATURES_POPULARITY,
    'mood_analysis': QUERY_MOOD_ANALYSIS,
    'explicit_analysis': QUERY_EXPLICIT_ANALYSIS,
    
    # Summary
    'summary_stats': QUERY_SUMMARY_STATS,
    'duration_analysis': QUERY_DURATION_ANALYSIS,
}
