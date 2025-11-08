import psycopg2
from psycopg2 import sql, OperationalError
import os
from dotenv import load_dotenv

load_dotenv()

# Thông tin kết nối
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def test_connection():
    try:
        # Tạo kết nối
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("✅ Kết nối thành công!")

        # Tạo cursor để thực thi lệnh SQL
        cur = conn.cursor()
        cur.execute("SELECT version();")  # kiểm tra phiên bản PostgreSQL
        version = cur.fetchone()
        print("PostgreSQL version:", version[0])

        # Đóng cursor và kết nối
        cur.close()
        conn.close()
    except OperationalError as e:
        print("❌ Không thể kết nối đến PostgreSQL")
        print(e)

if __name__ == "__main__":
    test_connection()
