# -*- coding: utf-8 -*-
"""
Script kiểm tra cấu trúc database
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cur = conn.cursor()
    
    # Check tables
    print("=" * 60)
    print("DANH SÁCH BẢNG TRONG DATABASE")
    print("=" * 60)
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = cur.fetchall()
    for table in tables:
        print(f"  - {table[0]}")
    
    # Check dim_song structure
    print("\n" + "=" * 60)
    print("CẤU TRÚC BẢNG: dim_song")
    print("=" * 60)
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = 'dim_song' 
        ORDER BY ordinal_position;
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:30} {row[1]:20} Nullable: {row[2]}")
    
    # Check dim_artist structure
    print("\n" + "=" * 60)
    print("CẤU TRÚC BẢNG: dim_artist")
    print("=" * 60)
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = 'dim_artist' 
        ORDER BY ordinal_position;
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:30} {row[1]:20} Nullable: {row[2]}")
    
    # Check fact_song_daily structure
    print("\n" + "=" * 60)
    print("CẤU TRÚC BẢNG: fact_song_daily")
    print("=" * 60)
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = 'fact_song_daily' 
        ORDER BY ordinal_position;
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:30} {row[1]:20} Nullable: {row[2]}")
    
    # Check fact_artist_stats structure
    print("\n" + "=" * 60)
    print("CẤU TRÚC BẢNG: fact_artist_stats")
    print("=" * 60)
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = 'fact_artist_stats' 
        ORDER BY ordinal_position;
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:30} {row[1]:20} Nullable: {row[2]}")
    
    # Check fact_audio_analysis structure
    print("\n" + "=" * 60)
    print("CẤU TRÚC BẢNG: fact_audio_analysis")
    print("=" * 60)
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = 'fact_audio_analysis' 
        ORDER BY ordinal_position;
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:30} {row[1]:20} Nullable: {row[2]}")
    
    # Check sample relationships
    print("\n" + "=" * 60)
    print("KIỂM TRA MỐI QUAN HỆ GIỮA CÁC BẢNG")
    print("=" * 60)
    
    # Check if song has direct artist reference
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'dim_song' AND column_name LIKE '%artist%';
    """)
    song_artist_cols = cur.fetchall()
    print(f"Columns có chứa 'artist' trong dim_song: {song_artist_cols if song_artist_cols else 'Không có'}")
    
    # Check for bridge table
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE '%song%artist%'
        OR table_name LIKE '%bridge%';
    """)
    bridge_tables = cur.fetchall()
    print(f"Bridge tables: {bridge_tables if bridge_tables else 'Không có'}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Lỗi: {e}")
