"""
Người số 1 - Twitch Extract Specialist
Chạy lệnh: python staging\\extract_twitch_data.py
Nhiệm vụ:
1. Gọi Twitch Helix API (streams/users/games) theo config
2. Ghi snapshot vào staging/rawData/twitch_streams_raw.csv
3. Log đầy đủ job/step vào Control DB
"""

import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from control.control_manager import ControlManager

# ======================
# HÀM 1: LẤY TOÀN BỘ STREAMS
# ======================
def get_twitch_streams(headers, streams_url, page_size, max_streams, delay_seconds):
    url = f"{streams_url}?first={page_size}"
    streams = []
    cursor = None

    while True:
        full_url = url + (f"&after={cursor}" if cursor else "")
        response = requests.get(full_url, headers=headers, timeout=30)
        data = response.json()

        if "data" not in data:
            print("Loi API hoac token het han:", data)
            break

        batch = data["data"]
        streams.extend(batch)
        print(f"Da lay {len(streams)} streams...")

        cursor = data.get("pagination", {}).get("cursor")
        if not cursor or len(streams) >= max_streams:
            break

        time.sleep(delay_seconds)

    df = pd.DataFrame(streams)
    print(f"Tong cong lay duoc {len(df)} streams.")
    return df


# ======================
# HÀM 2: LẤY THÔNG TIN USER
# ======================
def get_user_info(user_ids, headers, users_url):
    user_info = []
    for i in range(0, len(user_ids), 100):
        ids = user_ids[i:i+100]
        url = users_url + "&".join([f"id={uid}" for uid in ids])
        r = requests.get(url, headers=headers, timeout=30).json()
        user_info.extend(r.get("data", []))
    return pd.DataFrame(user_info)


# ======================
# HÀM 3: LẤY THÔNG TIN GAME
# ======================
def get_game_info(game_ids, headers, games_url):
    game_info = []
    for i in range(0, len(game_ids), 100):
        ids = game_ids[i:i+100]
        url = games_url + "&".join([f"id={gid}" for gid in ids])
        r = requests.get(url, headers=headers, timeout=30).json()
        game_info.extend(r.get("data", []))
    return pd.DataFrame(game_info)


# ======================
# HÀM 4: TẠO DATASET HOÀN CHỈNH
# ======================
def build_twitch_dataset(config, control: ControlManager):
    headers = {
        "Client-ID": config["client_id"],
        "Authorization": f"Bearer {config['access_token']}"
    }
    max_streams = int(config.get("max_streams", 2000))
    page_size = int(config.get("page_size", 100))
    delay_seconds = float(config.get("delay_seconds", 1))

    # Bước 1.2.1 (Người 1) - Khởi tạo step fetch_streams từ Twitch API
    control.start_step("fetch_streams")
    streams = get_twitch_streams(
        headers=headers,
        streams_url=config["streams_url"],
        page_size=page_size,
        max_streams=max_streams,
        delay_seconds=delay_seconds
    )
    control.finish_step("fetch_streams", records_processed=len(streams))
    print(f"Lay duoc {len(streams)} streams dang phat")
    if streams.empty:
        return pd.DataFrame()

    user_ids = streams["user_id"].astype(str).tolist()
    # Bước 1.2.2 (Người 1) - Lấy thông tin user chi tiết
    control.start_step("fetch_users")
    users = get_user_info(user_ids, headers=headers, users_url=config["users_url"])
    control.finish_step("fetch_users", records_processed=len(users))
    print(f"Lay duoc thong tin {len(users)} user")

    game_ids = streams["game_id"].dropna().astype(str).tolist()
    # Bước 1.2.3 (Người 1) - Lấy thông tin game chi tiết
    control.start_step("fetch_games")
    games = get_game_info(game_ids, headers=headers, games_url=config["games_url"])
    control.finish_step("fetch_games", records_processed=len(games))
    print(f"Lay duoc thong tin {len(games)} game")

    # Bước 1.2.4 (Người 1) - Merge streams/users/games để tạo snapshot hoàn chỉnh
    control.start_step("merge_dataset")
    df = streams.merge(users, left_on="user_id", right_on="id", how="left", suffixes=("", "_user"))
    df = df.merge(games, left_on="game_id", right_on="id", how="left", suffixes=("", "_game"))

    # Bước 1.2.5 (Người 1) - Chuẩn hóa cột output trước khi lưu CSV
    df_final = pd.DataFrame({
        "stream_id": df["id"],
        "streamer_name": df["user_name"],
        "platform": "Twitch",
        "game_name": df["name"],
        "category": df["type"],
        "viewer_count": df["viewer_count"],
        "follower_count": df["view_count"],
        "language": df["language"],
        "start_time": df["started_at"],
        "capture_time": datetime.now(timezone.utc).isoformat(),
        "stream_title": df["title"],
        "platform_id": df["user_id"],
        "game_id": df["game_id"]
    })
    control.finish_step("merge_dataset", records_processed=len(df_final))

    return df_final


# ======================
# HÀM 5: XÁC ĐỊNH ĐƯỜNG DẪN OUTPUT
# ======================
def resolve_output_path(config, base_dir):
    default_dir = os.path.join(base_dir, "rawData")
    output_dir = config.get("output_dir", default_dir)
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(base_dir, output_dir)
    os.makedirs(output_dir, exist_ok=True)

    output_file = config.get("output_filename")
    if not output_file:
        raise KeyError("Vui long cau hinh 'output_filename' cho source Twitch trong Control DB")
    return os.path.join(output_dir, output_file)


# ======================
# MAIN (CHẠY ĐỘC LẬP)
# ======================
if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    with ControlManager("extract_twitch_data") as control:
        twitch_config = control.get_source_config(
            "twitch",
            required_keys=[
                "client_id",
                "access_token",
                "streams_url",
                "users_url",
                "games_url",
                "output_filename",
            ],
        )

        df_twitch = build_twitch_dataset(twitch_config, control)
        if df_twitch.empty:
            control.log_event("no_data", "Khong co stream nao duoc tra ve", {})
            control.mark_job_failed("Khong co du lieu Twitch de luu")
            raise SystemExit("Khong co du lieu Twitch")

        # Bước 1.2.6 (Người 1) - Lưu snapshot Twitch ra CSV và log file audit
        output_path = resolve_output_path(twitch_config, BASE_DIR)
        df_twitch.to_csv(output_path, index=False, encoding="utf-8")
        control.log_file(output_path, len(df_twitch), status="loaded")
        control.log_event(
            "file_written",
            "Da ghi file Twitch",
            {"path": output_path, "records": len(df_twitch)}
        )
        print(f"File du lieu Twitch da duoc luu tai: {output_path}")
        control.mark_job_completed(records_processed=len(df_twitch))
