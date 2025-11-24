import os
import sys
from flask import Flask, render_template, jsonify
from datetime import datetime
import psycopg2
import yaml

# Import control utilities
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "staging", "control"))
try:
    from control_connect import (  # type: ignore
        get_recent_jobs, get_recent_batches, get_file_audit_logs,
        get_etl_statistics
    )
    CONTROL_AVAILABLE = True
except ImportError:
    CONTROL_AVAILABLE = False
    print("Warning: Control database utilities not available")

app = Flask(__name__)

# Doc file cau hinh YAML
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # => D:\DW\bi
CONFIG_PATH = os.path.join(BASE_DIR, "..", "staging", "config_dw.yaml")

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"Khong tim thay file cau hinh tai: {CONFIG_PATH}")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Thong tin ket noi Data Warehouse
dw_info = config["dw_db"]


def get_connection():
    return psycopg2.connect(
        host=dw_info["host"],
        port=dw_info["port"],
        dbname=dw_info["dbname"],
        user=dw_info["user"],
        password=dw_info["password"],
    )


def rows_to_dicts(cursor, rows):
    columns = [col[0] for col in cursor.description]
    result = []
    for row in rows:
        entry = {}
        for column, value in zip(columns, row):
            if hasattr(value, "isoformat"):
                value = value.isoformat()
            entry[column] = value
        result.append(entry)
    return result


@app.route("/")
def index():
    template_path = os.path.join(BASE_DIR, "dasboard", "index.html")
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            return f.read()
    return render_template("index.html")


@app.route("/api/summary_cards")
def summary_cards():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                COALESCE(SUM(viewers), 0) AS total_viewers,
                COUNT(*) AS total_streams,
                COUNT(DISTINCT game_id) AS total_games,
                COUNT(DISTINCT streamer_id) AS total_streamers
            FROM fact_stream_snapshot
            WHERE capture_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours';
            """
        )
        row = cur.fetchone()
        data = {
            "total_viewers": row[0] if row and row[0] is not None else 0,
            "total_streams": row[1] if row and row[1] is not None else 0,
            "total_games": row[2] if row and row[2] is not None else 0,
            "total_streamers": row[3] if row and row[3] is not None else 0,
        }
        return jsonify(data)
    finally:
        cur.close()
        conn.close()

@app.route("/api/platform_breakdown")
def platform_breakdown():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                p.platform_name,
                COALESCE(SUM(f.viewers), 0) AS total_viewers,
                COUNT(*) AS total_streams
            FROM fact_stream_snapshot f
            JOIN dim_platform p ON f.platform_id = p.platform_id
            WHERE f.capture_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            GROUP BY p.platform_name
            ORDER BY total_viewers DESC;
            """
        )
        rows = cur.fetchall()
        data = [
            {
                "platform_name": row[0],
                "viewers": row[1],
                "streams": row[2],
            }
            for row in rows
        ]
        return jsonify(data)
    finally:
        cur.close()
        conn.close()


@app.route("/api/top_games_recent")
def top_games_recent():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                g.game_name,
                COALESCE(SUM(f.viewers), 0) AS total_viewers,
                COUNT(*) AS total_streams
            FROM fact_stream_snapshot f
            JOIN dim_game g ON f.game_id = g.game_id
            WHERE f.capture_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            GROUP BY g.game_name
            ORDER BY total_viewers DESC
            LIMIT 15;
            """
        )
        rows = cur.fetchall()
        data = [
            {
                "game": row[0],
                "viewers": row[1],
                "streams": row[2],
            }
            for row in rows
        ]
        return jsonify(data)
    finally:
        cur.close()
        conn.close()


@app.route("/api/top_streamers_recent")
def top_streamers_recent():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                s.streamer_name,
                p.platform_name,
                COALESCE(SUM(f.viewers), 0) AS total_viewers,
                COUNT(*) AS total_streams
            FROM fact_stream_snapshot f
            JOIN dim_streamer s ON f.streamer_id = s.streamer_id
            JOIN dim_platform p ON f.platform_id = p.platform_id
            WHERE f.capture_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            GROUP BY s.streamer_name, p.platform_name
            ORDER BY total_viewers DESC
            LIMIT 15;
            """
        )
        rows = cur.fetchall()
        data = [
            {
                "streamer": row[0],
                "platform": row[1],
                "viewers": row[2],
                "streams": row[3],
            }
            for row in rows
        ]
        return jsonify(data)
    finally:
        cur.close()
        conn.close()


@app.route("/api/top_games_daily")
def top_games_daily():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                g.game_name,
                DATE(f.capture_time) AS capture_day,
                COALESCE(SUM(f.viewers), 0) AS total_viewers,
                COUNT(*) AS total_streams
            FROM fact_stream_snapshot f
            JOIN dim_game g ON f.game_id = g.game_id
            WHERE f.capture_time >= CURRENT_DATE - INTERVAL '14 days'
            GROUP BY g.game_name, DATE(f.capture_time)
            ORDER BY capture_day DESC, total_viewers DESC
            LIMIT 100;
            """
        )
        rows = cur.fetchall()
        data = [
            {
                "game": row[0],
                "day": row[1].isoformat() if row[1] else None,
                "viewers": row[2],
                "streams": row[3],
            }
            for row in rows
        ]
        return jsonify(data)
    finally:
        cur.close()
        conn.close()


@app.route("/api/viewer_trends")
def viewer_trends():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                DATE_TRUNC('hour', capture_time) AS time_bucket,
                COALESCE(SUM(viewers), 0) AS total_viewers,
                COUNT(*) AS stream_count
            FROM fact_stream_snapshot
            WHERE capture_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            GROUP BY DATE_TRUNC('hour', capture_time)
            ORDER BY time_bucket;
            """
        )
        rows = cur.fetchall()
        data = [
            {
                "time": row[0].isoformat() if row[0] else None,
                "viewers": row[1],
                "streams": row[2],
            }
            for row in rows
        ]
        return jsonify(data)
    finally:
        cur.close()
        conn.close()


@app.route("/api/top_games")
def top_games():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                g.game_name,
                COALESCE(SUM(f.viewers), 0) AS total_viewers
            FROM fact_stream_snapshot f
            JOIN dim_game g ON f.game_id = g.game_id
            WHERE f.capture_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            GROUP BY g.game_name
            ORDER BY total_viewers DESC
            LIMIT 10;
            """
        )
        rows = cur.fetchall()
        data = [
            {
                "game": row[0],
                "viewers": row[1],
            }
            for row in rows
        ]
        return jsonify(data)
    finally:
        cur.close()
        conn.close()

@app.route("/api/last_update")
def last_update():
    """Trả về thời gian cập nhật dữ liệu cuối cùng"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT MAX(capture_time) AS last_capture_time,
                   MAX(created_at) AS last_created_at
            FROM fact_stream_snapshot
            WHERE capture_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours';
            """
        )
        row = cur.fetchone()
        # Xử lý datetime từ database
        last_capture = None
        last_created = None
        if row and row[0]:
            if hasattr(row[0], 'isoformat'):
                last_capture = row[0].isoformat()
            else:
                last_capture = str(row[0])
        if row and row[1]:
            if hasattr(row[1], 'isoformat'):
                last_created = row[1].isoformat()
            else:
                last_created = str(row[1])
        
        data = {
            "last_capture_time": last_capture,
            "last_created_at": last_created,
            "server_time": datetime.now().isoformat()
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e), "last_capture_time": None, "last_created_at": None, "server_time": datetime.now().isoformat()})
    finally:
        cur.close()
        conn.close()


@app.route("/api/etl/jobs")
def etl_jobs():
    """Lấy danh sách ETL jobs gần đây"""
    if not CONTROL_AVAILABLE:
        return jsonify({"error": "Control database not available", "jobs": []})
    try:
        jobs = get_recent_jobs(limit=50)
        return jsonify({"jobs": jobs})
    except Exception as e:
        return jsonify({"error": str(e), "jobs": []})


@app.route("/api/etl/batches")
def etl_batches():
    """Lấy danh sách batch runs gần đây"""
    if not CONTROL_AVAILABLE:
        return jsonify({"error": "Control database not available", "batches": []})
    try:
        batches = get_recent_batches(limit=50)
        return jsonify({"batches": batches})
    except Exception as e:
        return jsonify({"error": str(e), "batches": []})


@app.route("/api/etl/files")
def etl_files():
    """Lấy danh sách file audit logs"""
    if not CONTROL_AVAILABLE:
        return jsonify({"error": "Control database not available", "files": []})
    try:
        files = get_file_audit_logs(limit=100)
        return jsonify({"files": files})
    except Exception as e:
        return jsonify({"error": str(e), "files": []})


@app.route("/api/etl/statistics")
def etl_statistics():
    """Lấy thống kê tổng quan về ETL"""
    if not CONTROL_AVAILABLE:
        return jsonify({"error": "Control database not available", "statistics": {}})
    try:
        stats = get_etl_statistics()
        return jsonify({"statistics": stats})
    except Exception as e:
        return jsonify({"error": str(e), "statistics": {}})


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
