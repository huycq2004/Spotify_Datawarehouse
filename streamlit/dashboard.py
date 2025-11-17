# -*- coding: utf-8 -*-
"""
Spotify Data Warehouse - Interactive Dashboard
Dashboard trực quan phân tích xu hướng âm nhạc và độ phổ biến nghệ sĩ toàn cầu
"""

import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from dotenv import load_dotenv
from sql_queries import ALL_QUERIES

# Load environment variables
load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Page config
st.set_page_config(
    page_title="Spotify Music Analytics",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 1rem 2rem;
    }
    
    /* Spacing between elements */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Metric boxes with gradient backgrounds */
    [data-testid="stMetricValue"] {
        font-size: 32px;
        font-weight: bold;
    }
    
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 30px 25px;
        border-radius: 18px;
        border: none;
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
        color: white !important;
        margin: 10px 0;
    }
    
    div[data-testid="metric-container"]:nth-child(1) {
        background: linear-gradient(135deg, #1DB954 0%, #169c46 100%);
    }
    
    div[data-testid="metric-container"]:nth-child(2) {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    
    div[data-testid="metric-container"]:nth-child(3) {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    }
    
    div[data-testid="metric-container"]:nth-child(4) {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }
    
    div[data-testid="metric-container"]:nth-child(5) {
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
    }
    
    div[data-testid="metric-container"]:nth-child(6) {
        background: linear-gradient(135deg, #30cfd0 0%, #330867 100%);
    }
    
    div[data-testid="metric-container"] label {
        color: white !important;
        font-weight: 600;
        font-size: 17px;
    }
    
    div[data-testid="metric-container"] [data-testid="stMetricValue"] {
        color: white !important;
    }
    
    div[data-testid="metric-container"] [data-testid="stMetricDelta"] {
        color: rgba(255,255,255,0.9) !important;
    }
    
    /* Row spacing */
    .row-widget {
        margin-bottom: 25px;
    }
    
    h1 {
        color: #1DB954;
        text-align: center;
        padding: 25px;
        margin-bottom: 30px;
    }
    h2 {
        color: #1DB954;
        border-bottom: 3px solid #1DB954;
        padding-bottom: 15px;
        margin-top: 30px;
        margin-bottom: 25px;
    }
    h3 {
        color: #1DB954;
        margin-top: 25px;
        margin-bottom: 20px;
    }
    
    /* Tabs styling with gradients */
    .stTabs [data-baseweb="tab-list"] {
        gap: 15px;
        background-color: transparent;
        padding: 15px 0;
    }
    .stTabs [data-baseweb="tab"] {
        height: 55px;
        background: linear-gradient(135deg, #e0e7ff 0%, #f0f4ff 100%);
        border-radius: 12px;
        padding: 12px 24px;
        font-weight: 500;
        border: 2px solid transparent;
        transition: all 0.3s ease;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background: linear-gradient(135deg, #c7d2fe 0%, #ddd6fe 100%);
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: linear-gradient(135deg, #1DB954 0%, #169c46 100%);
        color: white;
        border: 2px solid #1DB954;
    }
    
    /* Divider styling */
    hr {
        margin-top: 30px;
        margin-bottom: 30px;
        border: none;
        border-top: 2px solid #e5e7eb;
    }
    </style>
    """, unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_database_connection():
    """Tạo kết nối đến PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        st.error(f"❌ Không thể kết nối database: {e}")
        return None

@st.cache_data(ttl=600)
def execute_query(_conn, query):
    """Thực thi query và trả về DataFrame"""
    try:
        df = pd.read_sql_query(query, _conn)
        return df
    except Exception as e:
        st.error(f"❌ Lỗi khi thực thi query: {e}")
        return None

# Main dashboard
def main():
    # Header
    st.markdown("<h1>🎵 SPOTIFY MUSIC ANALYTICS DASHBOARD</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; font-size: 18px; color: #666;'>Phân tích xu hướng âm nhạc và độ phổ biến nghệ sĩ toàn cầu</p>", unsafe_allow_html=True)
    
    # Get database connection
    conn = get_database_connection()
    if conn is None:
        st.stop()
    
    # Sidebar
    with st.sidebar:
        st.image("https://storage.googleapis.com/pr-newsroom-wp/1/2018/11/Spotify_Logo_RGB_Green.png", width=200)
        st.markdown("---")
        st.markdown("### 📊 Navigation")
        st.markdown("""
        - 🌍 Tổng quan toàn cầu
        - 🎤 Phân tích nghệ sĩ
        - 🎵 Xu hướng âm nhạc
        - 🌏 Phân tích khu vực
        - 📅 Phân tích thời gian
        - 💿 Album & Audio Features
        """)
        st.markdown("---")
        st.markdown("### ℹ️ About")
        st.info("Dashboard này phân tích dữ liệu từ Spotify bao gồm 72 quốc gia, hơn 2 triệu bản ghi về bài hát, nghệ sĩ, và album.")
    
    # Summary metrics
    st.markdown("## 📈 Tổng quan Thống kê")
    df_summary = execute_query(conn, ALL_QUERIES['summary_stats'])
    
    if df_summary is not None and not df_summary.empty:
        # First row - main metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_songs = int(df_summary['total_songs'].values[0]) if df_summary['total_songs'].values[0] else 0
            st.metric("🎵 Tổng số bài hát", f"{total_songs:,}")
        with col2:
            total_artists = int(df_summary['total_artists'].values[0]) if df_summary['total_artists'].values[0] else 0
            st.metric("🎤 Tổng số nghệ sĩ", f"{total_artists:,}")
        with col3:
            total_countries = int(df_summary['total_countries'].values[0]) if df_summary['total_countries'].values[0] else 0
            st.metric("🌍 Số quốc gia", f"{total_countries:,}")
        
        # Second row - additional metrics
        col4, col5, col6 = st.columns(3)
        
        with col4:
            total_albums = int(df_summary['total_albums'].values[0]) if df_summary['total_albums'].values[0] else 0
            st.metric("💿 Tổng số album", f"{total_albums:,}")
        with col5:
            avg_pop = float(df_summary['avg_popularity'].values[0]) if df_summary['avg_popularity'].values[0] else 0.0
            st.metric("⭐ Độ phổ biến TB", f"{avg_pop:.1f}")
        with col6:
            max_pop = float(df_summary['max_popularity'].values[0]) if df_summary['max_popularity'].values[0] else 0.0
            st.metric("🔥 Độ phổ biến Max", f"{max_pop:.0f}")
    
    st.markdown("---")
    
    # Tabs for different analysis sections
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🌍 Xu hướng Toàn cầu",
        "🎤 Phân tích Nghệ sĩ",
        "🌏 Phân tích Khu vực",
        "📅 Phân tích Thời gian",
        "💿 Album & Thể loại",
        "🎶 Audio Features"
    ])
    
    # TAB 1: Global Trends
    with tab1:
        st.markdown("## 🌍 Xu hướng Âm nhạc Toàn cầu")
        
        # Top songs global
        st.markdown("### 🏆 Top 20 Bài hát Phổ biến nhất Toàn cầu")
        df_top_songs = execute_query(conn, ALL_QUERIES['top_songs_global'])
        
        if df_top_songs is not None and not df_top_songs.empty:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Bar chart
                fig = px.bar(df_top_songs.head(15), 
                           x='avg_popularity', 
                           y='song_name',
                           orientation='h',
                           title='Top 15 Bài hát theo Độ phổ biến',
                           labels={'avg_popularity': 'Độ phổ biến trung bình', 'song_name': 'Tên bài hát'},
                           color='avg_popularity',
                           color_continuous_scale='Viridis',
                           text='avg_popularity')
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                fig.update_layout(height=600, yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                # Data table
                st.dataframe(
                    df_top_songs[['song_name', 'artist_name', 'num_countries', 'avg_popularity']].head(20),
                    height=600,
                    hide_index=True
                )
        
        st.markdown("---")
        
        # Music category trends (based on audio features)
        st.markdown("### 🎸 Xu hướng theo Phân loại Âm nhạc")
        df_genre = execute_query(conn, ALL_QUERIES['genre_trends'])
        
        if df_genre is not None and not df_genre.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Pie chart
                fig = px.pie(df_genre.head(10), 
                           values='num_songs', 
                           names='music_category',
                           title='Phân bố Phân loại Âm nhạc (Top 10)',
                           color_discrete_sequence=px.colors.qualitative.Set3)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                # Bar chart
                fig = px.bar(df_genre.head(10), 
                           x='music_category', 
                           y='avg_popularity',
                           title='Độ phổ biến theo Phân loại',
                           labels={'avg_popularity': 'Độ phổ biến TB', 'music_category': 'Phân loại'},
                           color='avg_popularity',
                           color_continuous_scale='Blues')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, width='stretch')
        
        st.markdown("---")
        
        # Audio features of trending songs
        st.markdown("### 🎧 Đặc điểm Âm thanh của Bài hát Trending")
        df_audio_trending = execute_query(conn, ALL_QUERIES['audio_features_trending'])
        
        if df_audio_trending is not None and not df_audio_trending.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Mood distribution
                mood_dist = df_audio_trending.groupby('mood')['song_count'].sum().reset_index()
                fig = px.bar(mood_dist, 
                           x='mood', 
                           y='song_count',
                           title='Phân bố Mood trong Bài hát Trending',
                           labels={'song_count': 'Số lượng', 'mood': 'Mood'},
                           color='mood',
                           color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                # Energy vs Danceability
                energy_dance = df_audio_trending.groupby(['energy_level', 'danceability_level'])['song_count'].sum().reset_index()
                fig = px.density_heatmap(energy_dance, 
                                       x='energy_level', 
                                       y='danceability_level',
                                       z='song_count',
                                       title='Energy vs Danceability',
                                       labels={'song_count': 'Số lượng'},
                                       color_continuous_scale='YlOrRd')
                st.plotly_chart(fig, width='stretch')
    
    # TAB 2: Artist Analysis
    with tab2:
        st.markdown("## 🎤 Phân tích Độ phổ biến Nghệ sĩ")
        
        # Top artists
        st.markdown("### 🌟 Top 20 Nghệ sĩ Phổ biến nhất")
        df_top_artists = execute_query(conn, ALL_QUERIES['top_artists'])
        
        if df_top_artists is not None and not df_top_artists.empty:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Bar chart
                fig = px.bar(df_top_artists.head(15), 
                           x='avg_artist_score', 
                           y='artist_name',
                           orientation='h',
                           title='Top 15 Nghệ sĩ theo Điểm số',
                           labels={'avg_artist_score': 'Điểm nghệ sĩ TB', 'artist_name': 'Tên nghệ sĩ'},
                           color='countries_present',
                           color_continuous_scale='Reds',
                           text='avg_artist_score')
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                fig.update_layout(height=600, yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                st.dataframe(
                    df_top_artists[['artist_name', 'total_songs', 'countries_present', 'avg_artist_score']].head(20),
                    height=600,
                    hide_index=True
                )
        
        st.markdown("---")
        
        # Global reach artists
        st.markdown("### 🌍 Nghệ sĩ có Độ phủ sóng Quốc tế cao nhất")
        df_global_reach = execute_query(conn, ALL_QUERIES['artists_global_reach'])
        
        if df_global_reach is not None and not df_global_reach.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Scatter plot
                fig = px.scatter(df_global_reach.head(20), 
                               x='num_countries', 
                               y='avg_popularity',
                               size='num_songs',
                               hover_name='artist_name',
                               title='Độ phủ sóng vs Độ phổ biến',
                               labels={'num_countries': 'Số quốc gia', 
                                     'avg_popularity': 'Độ phổ biến TB',
                                     'num_songs': 'Số bài hát'},
                               color='num_countries',
                               color_continuous_scale='Viridis')
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                # Bar chart
                fig = px.bar(df_global_reach.head(15), 
                           x='artist_name', 
                           y='num_countries',
                           title='Top 15 Nghệ sĩ theo Số quốc gia',
                           labels={'num_countries': 'Số quốc gia', 'artist_name': 'Nghệ sĩ'},
                           color='avg_popularity',
                           color_continuous_scale='Blues')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, width='stretch')
        
        st.markdown("---")
        
        # Artist followers analysis
        st.markdown("### 👥 Phân tích Nghệ sĩ theo Số Bài hát")
        df_followers = execute_query(conn, ALL_QUERIES['artist_followers'])
        
        if df_followers is not None and not df_followers.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Artist tier distribution
                tier_dist = df_followers.groupby('artist_tier').size().reset_index(name='count')
                fig = px.pie(tier_dist, 
                           values='count', 
                           names='artist_tier',
                           title='Phân bố Nghệ sĩ theo Tier',
                           color_discrete_sequence=px.colors.qualitative.Set2)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                # Top artists by number of songs
                fig = px.bar(df_followers.head(15), 
                           x='artist_name', 
                           y='num_songs',
                           title='Top 15 Nghệ sĩ theo Số Bài hát',
                           labels={'num_songs': 'Số bài hát', 'artist_name': 'Nghệ sĩ'},
                           color='avg_song_popularity',
                           color_continuous_scale='Oranges')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, width='stretch')
        
        # Trending artists
        st.markdown("### 📈 Nghệ sĩ đang Trending (Tăng trưởng nhanh)")
        df_trending_artists = execute_query(conn, ALL_QUERIES['trending_artists'])
        
        if df_trending_artists is not None and not df_trending_artists.empty:
            fig = px.bar(df_trending_artists.head(15), 
                       x='artist_name', 
                       y='popularity_growth',
                       title='Top 15 Nghệ sĩ có Mức tăng trưởng cao nhất',
                       labels={'popularity_growth': 'Mức tăng độ phổ biến', 'artist_name': 'Nghệ sĩ'},
                       color='current_popularity',
                       color_continuous_scale='Greens',
                       text='popularity_growth')
            fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("📊 Không có dữ liệu nghệ sĩ trending trong khoảng thời gian gần đây. Cần ít nhất 60 ngày dữ liệu với mức tăng trưởng > 5 điểm.")
    
    # TAB 3: Regional Analysis
    with tab3:
        st.markdown("## 🌏 Phân tích theo Khu vực & Quốc gia")
        
        # Popularity by continent
        st.markdown("### 🌍 So sánh Độ phổ biến giữa các Quốc gia (Top 15)")
        df_continent = execute_query(conn, ALL_QUERIES['popularity_by_continent'])
        
        if df_continent is not None and not df_continent.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Bar chart
                fig = px.bar(df_continent, 
                           x='region', 
                           y='avg_popularity',
                           title='Độ phổ biến Trung bình theo Quốc gia',
                           labels={'avg_popularity': 'Độ phổ biến TB', 'region': 'Quốc gia'},
                           color='avg_popularity',
                           color_continuous_scale='Teal',
                           text='avg_popularity')
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                # Metrics per region
                fig = go.Figure()
                fig.add_trace(go.Bar(name='Bài hát', x=df_continent['region'], y=df_continent['unique_songs']))
                fig.add_trace(go.Bar(name='Nghệ sĩ', x=df_continent['region'], y=df_continent['unique_artists']))
                fig.update_layout(
                    title='Số lượng Bài hát & Nghệ sĩ theo Quốc gia',
                    xaxis_title='Quốc gia',
                    yaxis_title='Số lượng',
                    barmode='group',
                    xaxis_tickangle=-45
                )
                st.plotly_chart(fig, width='stretch')
        
        st.markdown("---")
        
        # Biggest music markets
        st.markdown("### 📊 Thị trường Âm nhạc Lớn nhất")
        df_markets = execute_query(conn, ALL_QUERIES['biggest_music_markets'])
        
        if df_markets is not None and not df_markets.empty:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Scatter plot
                fig = px.scatter(df_markets.head(25), 
                               x='unique_songs_in_chart', 
                               y='avg_popularity',
                               size='unique_artists',
                               hover_name='country_name',
                               title='Thị trường theo Số bài hát & Độ phổ biến',
                               labels={'unique_songs_in_chart': 'Số bài hát trong chart', 
                                     'avg_popularity': 'Độ phổ biến TB'},
                               color='unique_songs_in_chart',
                               color_continuous_scale='Blues')
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                st.dataframe(
                    df_markets[['country_name', 'unique_songs_in_chart', 'avg_popularity']].head(25),
                    height=400,
                    hide_index=True
                )
        
        st.markdown("---")
        
        # Regional music preferences
        st.markdown("### 🎵 Sở thích Âm nhạc theo Quốc gia (Top 10)")
        df_regional_pref = execute_query(conn, ALL_QUERIES['regional_music_preferences'])
        
        if df_regional_pref is not None and not df_regional_pref.empty:
            # Group by region and mood - show top 10 countries
            mood_region = df_regional_pref.groupby(['region', 'mood'])['song_count'].sum().reset_index()
            top_regions = mood_region.groupby('region')['song_count'].sum().nlargest(10).index
            mood_region_top = mood_region[mood_region['region'].isin(top_regions)]
            
            fig = px.bar(mood_region_top, 
                       x='region', 
                       y='song_count',
                       color='mood',
                       title='Phân bố Mood theo Quốc gia (Top 10)',
                       labels={'song_count': 'Số lượng bài hát', 'region': 'Quốc gia'},
                       barmode='group',
                       color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, width='stretch')
    
    # TAB 4: Time Analysis
    with tab4:
        st.markdown("## 📅 Phân tích theo Thời gian")
        
        # Popularity by weekday
        st.markdown("### 📆 Xu hướng theo Ngày trong Tuần")
        df_weekday = execute_query(conn, ALL_QUERIES['popularity_by_weekday'])
        
        if df_weekday is not None and not df_weekday.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.line(df_weekday, 
                            x='day_name', 
                            y='avg_popularity',
                            title='Độ phổ biến TB theo Ngày trong Tuần',
                            labels={'avg_popularity': 'Độ phổ biến TB', 'day_name': 'Ngày'},
                            markers=True)
                fig.update_traces(line_color='#1DB954', line_width=3, marker_size=10)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                fig = px.bar(df_weekday, 
                           x='day_name', 
                           y='num_songs',
                           title='Số lượng Bài hát theo Ngày',
                           labels={'num_songs': 'Số bài hát', 'day_name': 'Ngày'},
                           color='num_songs',
                           color_continuous_scale='Blues')
                st.plotly_chart(fig, width='stretch')
        
        st.markdown("---")
        
        # Popularity by month
        st.markdown("### 📊 Xu hướng theo Tháng")
        df_month = execute_query(conn, ALL_QUERIES['popularity_by_month'])
        
        if df_month is not None and not df_month.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Bar chart by month
                fig = px.bar(df_month, 
                           x='month_name', 
                           y='avg_popularity',
                           title='Độ phổ biến Trung bình theo Tháng',
                           labels={'avg_popularity': 'Độ phổ biến TB', 'month_name': 'Tháng'},
                           color='avg_popularity',
                           color_continuous_scale='RdYlGn',
                           text='avg_popularity')
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                fig.update_layout(xaxis_tickangle=-45, showlegend=False)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                # Number of songs and artists by month
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_month['month_name'], 
                    y=df_month['num_songs'],
                    mode='lines+markers',
                    name='Số bài hát',
                    line=dict(color='#1DB954', width=3),
                    marker=dict(size=8)
                ))
                fig.add_trace(go.Scatter(
                    x=df_month['month_name'], 
                    y=df_month['num_artists'],
                    mode='lines+markers',
                    name='Số nghệ sĩ',
                    line=dict(color='#FF6B6B', width=3),
                    marker=dict(size=8)
                ))
                fig.update_layout(
                    title='Số lượng Bài hát & Nghệ sĩ theo Tháng',
                    xaxis_title='Tháng',
                    yaxis_title='Số lượng',
                    hovermode='x unified',
                    xaxis_tickangle=-45,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig, width='stretch')
        
        st.markdown("---")
        
        # Longest #1 songs
        st.markdown("### 🏆 Bài hát giữ vị trí #1 Lâu nhất")
        df_longest = execute_query(conn, ALL_QUERIES['longest_number_one'])
        
        if df_longest is not None and not df_longest.empty:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                fig = px.bar(df_longest.head(15), 
                           x='days_at_number_one', 
                           y='song_name',
                           orientation='h',
                           title='Top 15 Bài hát giữ #1 Lâu nhất',
                           labels={'days_at_number_one': 'Số ngày ở #1', 'song_name': 'Bài hát'},
                           color='days_at_number_one',
                           color_continuous_scale='Reds',
                           text='days_at_number_one')
                fig.update_traces(texttemplate='%{text} days', textposition='outside')
                fig.update_layout(height=600, yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                st.dataframe(
                    df_longest[['song_name', 'artist_name', 'country_name', 'days_at_number_one']].head(20),
                    height=600,
                    hide_index=True
                )
    
    # TAB 5: Album & Genre Analysis
    with tab5:
        st.markdown("## 💿 Phân tích Album & Thể loại")
        
        # Top albums
        st.markdown("### 🎵 Top 20 Album Phổ biến nhất")
        df_top_albums = execute_query(conn, ALL_QUERIES['top_albums'])
        
        if df_top_albums is not None and not df_top_albums.empty:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                fig = px.bar(df_top_albums.head(15), 
                           x='avg_popularity', 
                           y='album_name',
                           orientation='h',
                           title='Top 15 Album theo Độ phổ biến',
                           labels={'avg_popularity': 'Độ phổ biến TB', 'album_name': 'Album'},
                           color='release_year',
                           color_continuous_scale='Viridis',
                           text='avg_popularity')
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                fig.update_layout(height=600, yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                st.dataframe(
                    df_top_albums[['album_name', 'artist_name', 'release_year', 'avg_popularity']].head(20),
                    height=600,
                    hide_index=True
                )
        
        st.markdown("---")
        
        # Album type analysis
        st.markdown("### 📀 Phân tích theo Loại Album")
        df_album_type = execute_query(conn, ALL_QUERIES['album_type_analysis'])
        
        if df_album_type is not None and not df_album_type.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(df_album_type, 
                           values='num_albums', 
                           names='album_type',
                           title='Phân bố theo Loại Album',
                           color_discrete_sequence=px.colors.qualitative.Set3)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                fig = px.bar(df_album_type, 
                           x='album_type', 
                           y='avg_popularity',
                           title='Độ phổ biến theo Loại Album',
                           labels={'avg_popularity': 'Độ phổ biến TB', 'album_type': 'Loại album'},
                           color='avg_popularity',
                           color_continuous_scale='Blues',
                           text='avg_popularity')
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                st.plotly_chart(fig, width='stretch')
        
        st.markdown("---")
        
        # Album release trends
        st.markdown("### 📅 Xu hướng Phát hành Album theo Năm")
        df_release_trends = execute_query(conn, ALL_QUERIES['album_release_trends'])
        
        if df_release_trends is not None and not df_release_trends.empty:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig.add_trace(
                go.Bar(name='Số Album', x=df_release_trends['release_year'], y=df_release_trends['num_albums']),
                secondary_y=False,
            )
            
            fig.add_trace(
                go.Scatter(name='Độ phổ biến TB', x=df_release_trends['release_year'], 
                         y=df_release_trends['avg_popularity'], mode='lines+markers',
                         line=dict(color='red', width=3)),
                secondary_y=True,
            )
            
            fig.update_xaxes(title_text="Năm")
            fig.update_yaxes(title_text="Số Album", secondary_y=False)
            fig.update_yaxes(title_text="Độ phổ biến TB", secondary_y=True)
            fig.update_layout(title_text="Xu hướng Phát hành Album & Độ phổ biến")
            
            st.plotly_chart(fig, width='stretch')
    
    # TAB 6: Audio Features
    with tab6:
        st.markdown("## 🎶 Phân tích Đặc điểm Âm thanh")
        
        # Audio features popularity
        st.markdown("### 🎧 Mối quan hệ giữa Đặc điểm Âm thanh và Độ phổ biến")
        df_audio_pop = execute_query(conn, ALL_QUERIES['audio_features_popularity'])
        
        if df_audio_pop is not None and not df_audio_pop.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Energy vs popularity
                energy_pop = df_audio_pop.groupby('energy_level').agg({
                    'avg_popularity': 'mean',
                    'song_count': 'sum'
                }).reset_index()
                
                fig = px.bar(energy_pop, 
                           x='energy_level', 
                           y='avg_popularity',
                           title='Độ phổ biến theo Mức Energy',
                           labels={'avg_popularity': 'Độ phổ biến TB', 'energy_level': 'Mức Energy'},
                           color='avg_popularity',
                           color_continuous_scale='Reds',
                           text='avg_popularity')
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                # Danceability vs popularity
                dance_pop = df_audio_pop.groupby('danceability_level').agg({
                    'avg_popularity': 'mean',
                    'song_count': 'sum'
                }).reset_index()
                
                fig = px.bar(dance_pop, 
                           x='danceability_level', 
                           y='avg_popularity',
                           title='Độ phổ biến theo Mức Danceability',
                           labels={'avg_popularity': 'Độ phổ biến TB', 'danceability_level': 'Mức Danceability'},
                           color='avg_popularity',
                           color_continuous_scale='Blues',
                           text='avg_popularity')
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                st.plotly_chart(fig, width='stretch')
        
        st.markdown("---")
        
        # Mood analysis
        st.markdown("### 😊 Phân tích Mood của Bài hát")
        df_mood = execute_query(conn, ALL_QUERIES['mood_analysis'])
        
        if df_mood is not None and not df_mood.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(df_mood, 
                           values='num_songs', 
                           names='mood',
                           title='Phân bố Mood trong Bài hát Phổ biến',
                           color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                fig = px.bar(df_mood, 
                           x='mood', 
                           y='avg_popularity',
                           title='Độ phổ biến theo Mood',
                           labels={'avg_popularity': 'Độ phổ biến TB', 'mood': 'Mood'},
                           color='avg_popularity',
                           color_continuous_scale='Greens',
                           text='avg_popularity')
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                st.plotly_chart(fig, width='stretch')
        
        st.markdown("---")
        
        # Explicit analysis
        st.markdown("### 🔞 Phân tích Bài hát Explicit vs Non-Explicit")
        df_explicit = execute_query(conn, ALL_QUERIES['explicit_analysis'])
        
        if df_explicit is not None and not df_explicit.empty:
            df_explicit['type'] = df_explicit['is_explicit'].map({True: 'Explicit', False: 'Non-Explicit'})
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(df_explicit, 
                           values='num_songs', 
                           names='type',
                           title='Tỷ lệ Explicit vs Non-Explicit',
                           color_discrete_sequence=['#FF6B6B', '#4ECDC4'])
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                fig = px.bar(df_explicit, 
                           x='type', 
                           y='avg_popularity',
                           title='So sánh Độ phổ biến',
                           labels={'avg_popularity': 'Độ phổ biến TB', 'type': 'Loại'},
                           color='avg_popularity',
                           color_continuous_scale='Oranges',
                           text='avg_popularity')
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                st.plotly_chart(fig, width='stretch')
        
        st.markdown("---")
        
        # Duration analysis
        st.markdown("### ⏱️ Phân tích theo Độ dài Bài hát")
        df_duration = execute_query(conn, ALL_QUERIES['duration_analysis'])
        
        if df_duration is not None and not df_duration.empty:
            fig = px.bar(df_duration, 
                       x='duration_category', 
                       y='avg_popularity',
                       title='Độ phổ biến theo Độ dài Bài hát',
                       labels={'avg_popularity': 'Độ phổ biến TB', 'duration_category': 'Độ dài'},
                       color='num_songs',
                       color_continuous_scale='Purples',
                       text='avg_popularity')
            fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
            st.plotly_chart(fig, width='stretch')
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 20px;'>
        <p>📊 Spotify Data Warehouse Analytics Dashboard</p>
        <p>🎵 Xây dựng kho dữ liệu, phân tích xu hướng âm nhạc và độ phổ biến nghệ sĩ toàn cầu từ Spotify</p>
        <p>Dữ liệu từ 72 quốc gia • Hơn 2 triệu bản ghi • Constellation Schema</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
