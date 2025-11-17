# 📊 Spotify Data Warehouse - Fact Tables & ETL Pipeline Documentation

## 📋 Mục lục
1. [Giới thiệu chung](#giới-thiệu-chung)
2. [Các Fact Tables](#các-fact-tables)
3. [Mô hình ETL](#mô-hình-etl)
4. [Ứng dụng thực tế](#ứng-dụng-thực-tế)
5. [Hướng dẫn sử dụng](#hướng-dẫn-sử-dụng)

---

## 🎯 Giới thiệu chung

**Spotify Data Warehouse** là hệ thống kho dữ liệu được thiết kế cho đồ án nhóm 4-5 sinh viên, sử dụng **Constellation Schema** (Galaxy Schema) với:
- **6 Dimension Tables** (Bảng chiều)
- **5 Fact Tables** (Bảng sự kiện)
- **Total: 11 Tables**

Warehouse này phân tích dữ liệu từ **72 quốc gia**, tracking **hơn 2 triệu bản ghi** về bài hát, nghệ sĩ, album, và các metrics Spotify.

---

## 📈 Các Fact Tables

### 1️⃣ **FACT_SONG_DAILY** - Hiệu suất Bài Hát Hàng Ngày

#### 🎯 Dùng để làm gì?
Theo dõi hiệu suất **từng bài hát** trên **từng quốc gia** theo **từng ngày**.

#### 📊 Cấu trúc dữ liệu:
```sql
fact_song_daily (
    fact_id              -- ID duy nhất
    song_id              -- Link đến bài hát
    date_id              -- Ngày (link đến dim_date)
    country_id           -- Quốc gia (link đến dim_country)
    album_id             -- Album (link đến dim_album)
    
    daily_rank           -- Xếp hạng hôm nay (1-100)
    popularity_score     -- Độ phổ biến (0-100)
    rank_points          -- Điểm xếp hạng (101 - rank)
    performance_index    -- Chỉ số hiệu suất = (rank_points + popularity) / 2
)
```

#### 💡 Ứng dụng thực tế:
```
✓ Theo dõi bài hát trending - Bài nào lên top 10 trong tuần?
✓ Phân tích xu hướng địa phương - Bài nào phổ biến ở VN, KR, US?
✓ Dự báo hit songs - Bài nào có trend tăng liên tục?
✓ So sánh hiệu suất - Bài A vs Bài B, ngày nào tốt nhất?
```

#### 🔍 Ví dụ Query:
```sql
-- Top 10 bài hát phổ biến nhất ở Việt Nam tuần này
SELECT s.song_name, a.artist_name, fs.daily_rank, fs.popularity_score
FROM fact_song_daily fs
JOIN dim_song s ON fs.song_id = s.song_id
JOIN dim_artist a ON s.song_id = a.artist_id
JOIN dim_country c ON fs.country_id = c.country_id
WHERE c.country_code = 'VN' 
  AND fs.date_id >= CURRENT_DATE - INTERVAL 7 DAY
ORDER BY fs.rank_points DESC
LIMIT 10;
```

---

### 2️⃣ **FACT_ARTIST_STATS** - Thống kê Nghệ Sĩ

#### 🎯 Dùng để làm gì?
Phân tích **hiệu suất từng nghệ sĩ** với **từng bài hát** theo **quốc gia** và **thời gian**.

#### 📊 Cấu trúc dữ liệu:
```sql
fact_artist_stats (
    fact_id              -- ID duy nhất
    artist_id            -- Nghệ sĩ
    song_id              -- Bài hát của nghệ sĩ
    date_id              -- Ngày
    country_id           -- Quốc gia
    
    song_rank            -- Xếp hạng bài hát
    song_popularity      -- Độ phổ biến bài hát
    artist_position      -- Vị trí của nghệ sĩ (1=chính, 2=featuring, ...)
    artist_score         -- Điểm nghệ sĩ
    contribution_weight  -- Trọng lượng đóng góp (1.0 cho main, 0.5 cho feat)
)
```

#### 💡 Ứng dụng thực tế:
```
✓ Xếp hạng nghệ sĩ - Ai là nghệ sĩ hot nhất tháng này?
✓ Phân tích collaboration - Featuring với ai tăng stream?
✓ Theo dõi sự phát triển - Trend của từng artist qua thời gian?
✓ So sánh đóng góp - Artist chính vs Featuring, ai có ảnh hưởng hơn?
```

#### 🔍 Ví dụ Query:
```sql
-- Top 5 nghệ sĩ có bài hát trending nhất
SELECT a.artist_name, COUNT(DISTINCT fs.song_id) as num_songs, 
       AVG(fs.artist_score) as avg_score
FROM fact_artist_stats fs
JOIN dim_artist a ON fs.artist_id = a.artist_id
WHERE fs.date_id >= CURRENT_DATE - INTERVAL 30 DAY
GROUP BY a.artist_name
ORDER BY avg_score DESC
LIMIT 5;
```

---

### 3️⃣ **FACT_CHART_POSITION** - Vị Trí & Di Chuyển Trên BXH

#### 🎯 Dùng để làm gì?
Theo dõi **sự thay đổi xếp hạng** (movement) của bài hát qua các ngày.

#### 📊 Cấu trúc dữ liệu:
```sql
fact_chart_position (
    fact_id              -- ID duy nhất
    song_id              -- Bài hát
    date_id              -- Ngày hiện tại
    country_id           -- Quốc gia
    
    current_rank         -- Xếp hạng hôm nay
    previous_rank        -- Xếp hạng hôm qua
    daily_movement       -- Thay đổi hôm nay (+5, -3, ...)
    weekly_movement      -- Thay đổi trong tuần
    
    is_rising            -- TRUE nếu bài đang tăng
    is_falling           -- TRUE nếu bài đang giảm
    movement_magnitude   -- Độ lớn thay đổi
    trend_strength       -- Độ mạnh của trend (0-10)
)
```

#### 💡 Ứng dụng thực tế:
```
✓ Phát hiện viral songs - Bài nào tăng nhanh nhất?
✓ Dự báo hit - Bài có trend tăng liên tục sẽ thành hit
✓ Phân tích momentum - Bài có momentum bao lâu?
✓ Cảnh báo giảm - Bài nào đang mất popularity?
✓ Phân tích chu kỳ - Bài có pattern lặp lại?
```

#### 🔍 Ví dụ Query:
```sql
-- Bài hát đang viral - tăng xếp hạng liên tục
SELECT s.song_name, a.artist_name, 
       cp.current_rank, cp.daily_movement, cp.trend_strength
FROM fact_chart_position cp
JOIN dim_song s ON cp.song_id = s.song_id
JOIN dim_artist a ON s.song_id = a.artist_id
WHERE cp.is_rising = TRUE 
  AND cp.trend_strength > 5
  AND cp.date_id = CURRENT_DATE
ORDER BY cp.trend_strength DESC;
```

---

### 4️⃣ **FACT_AUDIO_ANALYSIS** - Phân Tích Đặc Điểm Âm Nhạc

#### 🎯 Dùng để làm gì?
Phân tích **đặc tính âm thanh** của mỗi bài hát (từ Spotify Audio Features API).

#### 📊 Cấu trúc dữ liệu:
```sql
fact_audio_analysis (
    fact_id              -- ID duy nhất
    song_id              -- Bài hát (UNIQUE - 1 bài = 1 record)
    features_id          -- Link đến dim_audio_features
    
    -- Các Spotify Audio Features (0-1 scale)
    danceability         -- Có nhảy được không? (0=khó, 1=dễ)
    energy               -- Cường độ như thế nào? (0=yên tĩnh, 1=nổi loạn)
    speechiness          -- Có nói nhiều không? (0=ít, 1=nhiều)
    acousticness         -- Bao nhiêu % là nhạc cụ acoustic? (0=điện, 1=acoustic)
    instrumentalness     -- Bao nhiêu % là nhạc cụ? (0=vocals, 1=instrument)
    liveness             -- Cảm giác live không? (0=studio, 1=live)
    valence              -- Mood vui hay buồn? (0=buồn, 1=vui)
    
    -- Derivatives
    key_signature        -- Bài hát ở key nào (0-11)
    mode                 -- Major (1) hay Minor (0)
    loudness             -- Độ lớn (dB)
    tempo                -- Tốc độ nhịp (BPM)
    time_signature       -- Nhịp 3/4, 4/4, ...
    
    -- Calculated Metrics
    energy_dance_score   -- Năng lượng + Danceability
    mood_score           -- Chỉ số tâm trạng
)
```

#### 💡 Ứng dụng thực tế:
```
✓ Tạo playlist tự động - Ghép nhạc có cùng "vibe"
✓ Recommend songs - "Bạn thích bài này, bạn sẽ thích bài kia"
✓ Mood detection - Bài nào hay nghe lúc buồn/vui/làm việc?
✓ Genre classification - Phân loại nhạc theo đặc tính
✓ Analyze trends - Nhạc trending hiện nay energetic hay chill?
```

#### 🔍 Ví dụ Query:
```sql
-- Tạo playlist "Workout" - nhạc có energy cao & danceability cao
SELECT s.song_name, a.artist_name, 
       fa.energy, fa.danceability, fa.tempo
FROM fact_audio_analysis fa
JOIN dim_song s ON fa.song_id = s.song_id
JOIN dim_artist a ON s.song_id = a.artist_id
WHERE fa.energy > 0.8 
  AND fa.danceability > 0.7
  AND fa.tempo > 120
ORDER BY fa.energy DESC, fa.tempo DESC
LIMIT 50;
```

---

### 5️⃣ **FACT_STREAMING_METRICS** - Metrics Streaming & Engagement

#### 🎯 Dùng để làm gì?
Theo dõi **streams, listeners, và engagement** của mỗi bài trên mỗi quốc gia mỗi ngày.

#### 📊 Cấu trúc dữ liệu:
```sql
fact_streaming_metrics (
    fact_id              -- ID duy nhất
    song_id              -- Bài hát
    date_id              -- Ngày
    country_id           -- Quốc gia
    
    -- Streaming Data (ước tính từ ranking)
    estimated_streams    -- Tổng lượt stream ước tính
    estimated_listeners  -- Số listener ước tính
    
    -- Engagement Metrics
    avg_completion_rate  -- % hoàn thành bài (0-100)
    engagement_score     -- Điểm engagement (0-100)
    viral_coefficient    -- Hệ số viral (0-1)
)
```

#### 💡 Ứng dụng thực tế:
```
✓ Dự báo revenue - Bài nào sinh ra nhiều đồng nhất?
✓ ROI analysis - Đầu tư marketing vào bài nào có lợi?
✓ Engagement tracking - Người nghe có engaged không?
✓ Viral analysis - Bài nào viral? Viral nhanh bao lâu?
✓ Growth projection - Bài sẽ đạt bao nhiêu streams?
```

#### 🔍 Ví dụ Query:
```sql
-- Top earner songs - Bài nào sinh ra nhiều stream?
SELECT s.song_name, a.artist_name, 
       SUM(sm.estimated_streams) as total_streams,
       COUNT(DISTINCT sm.country_id) as countries
FROM fact_streaming_metrics sm
JOIN dim_song s ON sm.song_id = s.song_id
JOIN dim_artist a ON s.song_id = a.artist_id
WHERE sm.date_id >= CURRENT_DATE - INTERVAL 90 DAY
GROUP BY s.song_name, a.artist_name
ORDER BY total_streams DESC
LIMIT 20;
```

---

## 🔄 Mô Hình ETL

### ETL là gì?
**ETL = Extract → Transform → Load**
- **E**xtract: Lấy dữ liệu từ nguồn (CSV file)
- **T**ransform: Làm sạch, validate, xử lý dữ liệu
- **L**oad: Đưa dữ liệu vào database

### 🏗️ Kiến trúc Quy Trình

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCE                              │
│        universal_top_spotify_songs.csv                      │
│        (72 quốc gia, ~2.088M bản ghi)                      │
└─────────────────────────────────────────────────────────────┘
                            ↓
                    ╔═════════════════╗
                    ║    EXTRACT      ║
                    ║  Đọc CSV file   ║
                    ║  chunks 10K     ║
                    ╚═════════════════╝
                            ↓
                    ╔═════════════════╗
                    ║   TRANSFORM     ║
                    ║  - Clean data   ║
                    ║  - Handle NULLs ║
                    ║  - Calculate    ║
                    ║  - Categorize   ║
                    ╚═════════════════╝
                            ↓
                ┌───────────────────────┐
                │    DIMENSION TABLES   │
                ├───────────────────────┤
                │ - dim_song            │
                │ - dim_artist          │
                │ - dim_album           │
                │ - dim_date            │
                │ - dim_country         │
                │ - dim_audio_features  │
                └───────────────────────┘
                            ↓
                    ╔═════════════════╗
                    ║      LOAD       ║
                    ║  - Insert dims  ║
                    ║  - Insert facts ║
                    ║  - Validate     ║
                    ╚═════════════════╝
                            ↓
                    ┌───────────────────┐
                    │   FACT TABLES     │
                    ├───────────────────┤
                    │ - fact_song_daily │
                    │ - fact_artist_    │
                    │   stats           │
                    │ - fact_chart_     │
                    │   position        │
                    │ - fact_audio_     │
                    │   analysis        │
                    │ - fact_streaming_ │
                    │   metrics         │
                    └───────────────────┘
```

### 🔧 Chi tiết quy trình Transform

#### Phase 1: Data Cleaning
```python
clean_text()              # Xóa khoảng trắng, format text
clean_numeric()           # Validate range 0-1 cho features
clean_boolean()           # Convert TRUE/FALSE
clean_date()              # Parse ngày tháng
```

#### Phase 2: Data Enrichment
```python
extract_and_clean_artists()  # Tách danh sách nghệ sĩ
categorize_mood()            # Phân loại mood từ valence + energy
categorize_audio_features()  # 5 categories từ audio features
```

#### Phase 3: NULL Handling
```
❌ Bỏ bản ghi thiếu: spotify_id, name, snapshot_date
✅ Default values:
   - duration_ms → 180,000 ms (3 phút)
   - popularity → 50
   - audio features → 0.5 (neutral)
   - daily_rank → 100
   - country → 'GLOBAL'
```

#### Phase 4: Deduplication
```python
Drop duplicates trên: (spotify_id, artists, snapshot_date, country)
```

### 📊 Data Flow Example

```
INPUT: 1 dòng CSV
┌────────────────────────────────────────────────────────┐
│ spotify_id, name, artists, country, snapshot_date,    │
│ energy, danceability, valence, ...                     │
└────────────────────────────────────────────────────────┘
                        ↓
                   TRANSFORM
                        ↓
OUTPUT: Dữ liệu trong 6 Dimension Tables + 5 Fact Tables
┌────────────────────────────────────────────────────────┐
│ dim_song (1 record)       → fact_song_daily (1-72)    │
│ dim_artist (1-5 records)  → fact_artist_stats (1-5)   │
│ dim_album (1 record)      → fact_chart_position (1)   │
│ dim_date (1 record)       → fact_audio_analysis (1)   │
│ dim_country (1-72)        → fact_streaming_metrics (1)│
│ dim_audio_features (1)                                │
└────────────────────────────────────────────────────────┘
```

---

## 🎓 Ứng dụng Thực Tế

### 📈 Scenario 1: Marketing Campaign - "Bài nào nên promote?"

```sql
-- Bài trending + engagement cao = nên invest marketing
SELECT TOP 10 
    s.song_name, 
    a.artist_name,
    cp.trend_strength,
    fa.energy,
    sm.engagement_score,
    COUNT(*) as countries_trending
FROM fact_chart_position cp
JOIN fact_audio_analysis fa ON cp.song_id = fa.song_id
JOIN fact_streaming_metrics sm ON cp.song_id = sm.song_id
JOIN dim_song s ON cp.song_id = s.song_id
JOIN dim_artist a ON s.song_id = a.artist_id
WHERE cp.is_rising = TRUE 
  AND cp.trend_strength > 7
  AND sm.engagement_score > 80
  AND cp.date_id = CURRENT_DATE
GROUP BY s.song_name, a.artist_name, cp.trend_strength, fa.energy, sm.engagement_score
ORDER BY cp.trend_strength DESC;
```

### 🎵 Scenario 2: Playlist Creation - "Tạo playlist Workout"

```sql
-- Nhạc energetic + danceability cao + fast tempo
SELECT TOP 50
    s.song_name,
    a.artist_name,
    fa.energy,
    fa.danceability,
    fa.tempo,
    fsd.popularity_score
FROM fact_audio_analysis fa
JOIN dim_song s ON fa.song_id = s.song_id
JOIN dim_artist a ON s.song_id = a.artist_id
JOIN fact_song_daily fsd ON fa.song_id = fsd.song_id
WHERE fa.energy > 0.8
  AND fa.danceability > 0.7
  AND fa.tempo > 130
  AND fsd.date_id = CURRENT_DATE
ORDER BY fa.energy DESC, fa.tempo DESC;
```

### 🌍 Scenario 3: Regional Analysis - "Nhạc nào phổ biến ở từng khu vực?"

```sql
-- Top songs by region
SELECT 
    c.country_name,
    s.song_name,
    a.artist_name,
    fsd.daily_rank,
    fsd.popularity_score
FROM fact_song_daily fsd
JOIN dim_song s ON fsd.song_id = s.song_id
JOIN dim_artist a ON s.song_id = a.artist_id
JOIN dim_country c ON fsd.country_id = c.country_id
WHERE fsd.date_id = CURRENT_DATE
  AND fsd.daily_rank <= 10
  AND c.country_code IN ('VN', 'KR', 'US', 'BR', 'IN')
ORDER BY c.country_code, fsd.daily_rank;
```

### 💡 Scenario 4: Artist Growth - "Nghệ sĩ nào đang tăng trưởng?"

```sql
-- Artist growth trend - so sánh tháng này vs tháng trước
SELECT 
    a.artist_name,
    COUNT(DISTINCT fas.song_id) as num_songs,
    AVG(fas.artist_score) as avg_score_this_month,
    (AVG(fas.artist_score) - LAG(AVG(fas.artist_score)) OVER (ORDER BY DATE_PART('month', d.full_date))) as growth
FROM fact_artist_stats fas
JOIN dim_artist a ON fas.artist_id = a.artist_id
JOIN dim_date d ON fas.date_id = d.date_id
WHERE DATE_PART('year', d.full_date) = YEAR(CURRENT_DATE)
GROUP BY a.artist_name, DATE_PART('month', d.full_date)
ORDER BY growth DESC;
```

---

## 💾 Hướng Dẫn Sử Dụng

### 1. Cài đặt Environment

```bash
# 1. Clone repository
git clone https://github.com/huycq2004/Spotify_Datawarehouse.git
cd DataWH

# 2. Cài đặt dependencies
pip install -r requirements.txt

# 3. Setup PostgreSQL
# - Tạo database: spotify_data_warehouse
# - User: postgres
# - Password: huytk123

# 4. Cấu hình .env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=spotify_data_warehouse
DB_USER=postgres
DB_PASS=huytk123
```

### 2. Chạy ETL Pipeline

```bash
python create_warehouse.py
```

**Output:**
```
================================================================================
  🎵 SPOTIFY DATA WAREHOUSE - STUDENT PROJECT VERSION
================================================================================
  📊 Schema: Constellation (Galaxy) Schema
  📋 Tables: 6 Dimensions + 5 Facts = 11 Tables
  👥 Phù hợp: Đồ án nhóm 4-5 sinh viên
================================================================================

✅ Schema created successfully!
   📊 6 Dimension Tables
   📈 5 Fact Tables
   📝 Total: 11 Tables

📥 EXTRACT: Đọc dữ liệu từ universal_top_spotify_songs.csv

🔄 TRANSFORM: Đang xử lý ... dòng dữ liệu

📤 LOAD DIMENSIONS:
   ✓ dim_song: X records
   ✓ dim_artist: X records
   ✓ dim_album: X records
   ✓ dim_date: X records
   ✓ dim_country: 72 records
   ✓ dim_audio_features: X feature combinations

📤 LOAD FACTS:
   ✓ fact_song_daily: X records
   ✓ fact_artist_stats: X records
   ✓ fact_chart_position: X records
   ✓ fact_audio_analysis: X records
   ✓ fact_streaming_metrics: X records

✅ ETL Pipeline Completed Successfully!
```

### 3. Query Dữ Liệu

Sử dụng `query_data.py`:

```bash
python query_data.py
```

Hoặc kết nối trực tiếp với PostgreSQL:

```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="spotify_data_warehouse",
    user="postgres",
    password="huytk123"
)

cur = conn.cursor()

# Query example
cur.execute("""
    SELECT s.song_name, a.artist_name, fsd.daily_rank
    FROM fact_song_daily fsd
    JOIN dim_song s ON fsd.song_id = s.song_id
    JOIN dim_artist a ON s.song_id = a.artist_id
    LIMIT 10
""")

for row in cur.fetchall():
    print(row)
```

---

## 📝 Dimension Tables (Bảng Chiều)

| Table | Dùng để | Records |
|-------|---------|---------|
| dim_song | Lưu thông tin bài hát | ~10K |
| dim_artist | Lưu thông tin nghệ sĩ | ~50K |
| dim_album | Lưu thông tin album | ~20K |
| dim_date | Lưu thông tin thời gian | ~365 |
| dim_country | Lưu thông tin quốc gia | 72 |
| dim_audio_features | Lưu phân loại đặc tính âm nhạc | Dynamic |

---

## 🎯 Kết Luận

**Spotify Data Warehouse** cung cấp:
- ✅ **Dữ liệu sạch & chuẩn hóa** từ 72 quốc gia
- ✅ **5 Fact Tables** cho các use case khác nhau
- ✅ **Mô hình ETL** đơn giản nhưng đủ mạnh
- ✅ **Dễ query** và analyze
- ✅ **Scalable** cho đồ án học tập

---

**Created by:** Sinh viên làm đồ án Kho Dữ Liệu  
**Version:** 1.0  
**Last Updated:** November 2025
