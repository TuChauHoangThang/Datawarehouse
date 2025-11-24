"""
Người số 1 - YouTube Extract Specialist
Chạy lệnh: python staging\\extract_youtube_data.py
Nhiệm vụ:
1. Tìm live gaming streams qua YouTube Data API
2. Lấy chi tiết video & kênh, hợp nhất dataset
3. Xuất staging/rawData/youtube_streams_raw.csv và log Control DB
"""

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from control.control_manager import ControlManager


def parse_config_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    value = str(value).strip()
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(v).strip() for v in parsed if str(v).strip()]
    except json.JSONDecodeError:
        pass
    return [item.strip() for item in value.split(",") if item.strip()]

# ======================
# HÀM 1: TÌM CÁC LIVE STREAMS/BROADCASTS
# ======================
def get_youtube_live_streams(
    api_key,
    search_endpoint,
    search_queries,
    region_code,
    video_category_id,
    max_results,
    delay_seconds,
    control: ControlManager = None,
):
    """
    Lấy danh sách các live streams đang phát trên YouTube
    YouTube API không có endpoint trực tiếp để lấy tất cả live streams,
    nên ta sẽ tìm các video đang live bằng cách search với eventType='live'
    """
    all_videos = {}
    total_requests = 0
    
    print("Dang tim cac live streams tren YouTube...")
    
    for query in search_queries:
        next_page_token = None
        max_pages = (max_results // 50) + 1  # Mỗi page có tối đa 50 results
        
        print(f"   Query: '{query}'")
        
        for page in range(max_pages):
            url = search_endpoint
            params = {
                "part": "snippet",
                "eventType": "live",  # Chỉ lấy video đang live
                "type": "video",
                "maxResults": 50,
                "order": "viewCount",  # Sắp xếp theo view count
                "q": query,
                "regionCode": region_code,
                "key": api_key
            }
            
            if video_category_id:
                params["videoCategoryId"] = video_category_id
            
            if next_page_token:
                params["pageToken"] = next_page_token
            
            try:
                response = requests.get(url, params=params, timeout=30)
                
                # Kiểm tra response trước khi raise_for_status
                if response.status_code == 403:
                    # Đọc error response từ YouTube API
                    try:
                        error_data = response.json()
                        if "error" in error_data:
                            error_info = error_data["error"]
                            error_reason = error_info.get("errors", [{}])[0].get("reason", "unknown")
                            error_message = error_info.get("message", "Forbidden")
                            
                            import sys
                            detailed_error = f"""
============================================================
LOI YOUTUBE API 403 - FORBIDDEN
============================================================
Ly do: {error_reason}
Thong bao: {error_message}

CAC NGUYEN NHAN CO THE:
1. API Key khong hop le hoac da bi vo hieu hoa
2. API Key chua duoc kich hoat YouTube Data API v3
3. Quota da het (10000 units/ngay cho free tier)
4. API Key bi han che (restricted) va chua duoc cau hinh dung
5. API Key khong co quyen truy cap

HUONG DAN SUA:
1. Kiem tra API Key tai: https://console.cloud.google.com/apis/credentials
2. Kich hoat YouTube Data API v3: https://console.cloud.google.com/apis/library/youtube.googleapis.com
3. Kiem tra quota: https://console.cloud.google.com/apis/api/youtube.googleapis.com/quotas
4. Neu API Key bi restricted, them domain/IP vao danh sach allowed
5. Tao API Key moi neu can
============================================================
"""
                            print(detailed_error, file=sys.stderr)
                            print(detailed_error)
                            if control:
                                control.log_event(
                                    "api_error",
                                    "YouTube API 403",
                                    {"query": query, "reason": error_reason, "message": error_message}
                                )
                            raise requests.exceptions.RequestException(f"403 Forbidden: {error_reason} - {error_message}")
                    except:
                        pass
                
                response.raise_for_status()
                data = response.json()
                total_requests += 1
                
                # Kiểm tra lỗi từ YouTube API
                if "error" in data:
                    import sys
                    error_info = data["error"]
                    error_reason = error_info.get("errors", [{}])[0].get("reason", "unknown")
                    error_msg = f"Loi tu YouTube API: {error_info.get('message', 'Unknown error')} (Code: {error_info.get('code', 'Unknown')}, Reason: {error_reason})"
                    print(error_msg, file=sys.stderr)
                    print(error_msg)
                    if control:
                        control.log_event(
                            "api_error",
                            "YouTube API tra ve loi",
                            {"query": query, "error": error_info}
                        )
                    # Nếu là lỗi quan trọng, raise exception
                    if error_info.get("code") in [401, 403, 429]:
                        raise requests.exceptions.RequestException(error_msg)
                    break
                
                if "items" not in data:
                    import sys
                    warning_msg = f"Khong tim thay live streams hoac API key khong hop le cho query '{query}'"
                    print(warning_msg, file=sys.stderr)
                    print(warning_msg)
                    if control:
                        control.log_event(
                            "api_warning",
                            warning_msg,
                            {"query": query}
                        )
                    break
                
                videos = data["items"]
                for item in videos:
                    video_id = item.get("id", {}).get("videoId")
                    if video_id and video_id not in all_videos:
                        all_videos[video_id] = item
                
                print(f"      Tong so live streams da lay: {len(all_videos)}")
                
                if len(all_videos) >= max_results:
                    break
                
                next_page_token = data.get("nextPageToken")
                if not next_page_token:
                    break
                
                # Rate limiting: YouTube API cho phép 100 units/second, mỗi search request = 100 units
                # Đợi 1 giây giữa các request để tránh vượt quota
                time.sleep(delay_seconds)
            
            except requests.exceptions.RequestException as e:
                import sys
                error_msg = f"Loi khi goi YouTube API (query='{query}'): {e}"
                print(error_msg, file=sys.stderr)
                print(error_msg)
                if control:
                    control.log_event(
                        "api_error",
                        "Loi khi goi YouTube search",
                        {"query": query, "error": str(e)}
                    )
                # Nếu là lỗi quan trọng (như API key invalid, forbidden, quota), nên raise để dừng script
                if "401" in str(e) or "403" in str(e) or "429" in str(e) or "quota" in str(e).lower() or "forbidden" in str(e).lower():
                    raise
                break
        
        if len(all_videos) >= max_results:
            break
    
    print(f"Tong cong tim duoc {len(all_videos)} live streams sau {total_requests} request.")
    return list(all_videos.values())


# ======================
# HÀM 2: LẤY THÔNG TIN CHI TIẾT VỀ VIDEO
# ======================
def get_video_details(video_ids, api_key, videos_endpoint, delay_seconds):
    """
    Lấy thông tin chi tiết về các video (view count, duration, etc.)
    """
    video_details = []
    
    # YouTube API chỉ cho phép lấy tối đa 50 video mỗi lần
    for i in range(0, len(video_ids), 50):
        ids_batch = video_ids[i:i+50]
        video_ids_str = ",".join(ids_batch)
        
        url = videos_endpoint
        params = {
            "part": "statistics,snippet,contentDetails,liveStreamingDetails",
            "id": video_ids_str,
            "key": api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Kiểm tra lỗi từ YouTube API
            if "error" in data:
                import sys
                error_info = data["error"]
                error_msg = f"Loi tu YouTube API khi lay chi tiet video: {error_info.get('message', 'Unknown error')} (Code: {error_info.get('code', 'Unknown')})"
                print(error_msg, file=sys.stderr)
                print(error_msg)
                if error_info.get("code") in [401, 403, 429]:
                    raise requests.exceptions.RequestException(error_msg)
            
            if "items" in data:
                video_details.extend(data["items"])
            
            time.sleep(delay_seconds)
            
        except requests.exceptions.RequestException as e:
            import sys
            error_msg = f"Loi khi lay chi tiet video: {e}"
            print(error_msg, file=sys.stderr)
            print(error_msg)
            if "401" in str(e) or "403" in str(e) or "quota" in str(e).lower():
                raise
    
    return pd.DataFrame(video_details)


# ======================
# HÀM 3: LẤY THÔNG TIN CHANNEL
# ======================
def get_channel_info(channel_ids, api_key, channels_endpoint, delay_seconds):
    """
    Lấy thông tin về các channel (subscriber count, etc.)
    """
    channel_info = []
    
    # YouTube API chỉ cho phép lấy tối đa 50 channel mỗi lần
    for i in range(0, len(channel_ids), 50):
        ids_batch = channel_ids[i:i+50]
        channel_ids_str = ",".join(ids_batch)
        
        url = channels_endpoint
        params = {
            "part": "statistics,snippet",
            "id": channel_ids_str,
            "key": api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Kiểm tra lỗi từ YouTube API
            if "error" in data:
                import sys
                error_info = data["error"]
                error_msg = f"Loi tu YouTube API khi lay thong tin channel: {error_info.get('message', 'Unknown error')} (Code: {error_info.get('code', 'Unknown')})"
                print(error_msg, file=sys.stderr)
                print(error_msg)
                if error_info.get("code") in [401, 403, 429]:
                    raise requests.exceptions.RequestException(error_msg)
            
            if "items" in data:
                channel_info.extend(data["items"])
            
            time.sleep(delay_seconds)
            
        except requests.exceptions.RequestException as e:
            import sys
            error_msg = f"Loi khi lay thong tin channel: {e}"
            print(error_msg, file=sys.stderr)
            print(error_msg)
            if "401" in str(e) or "403" in str(e) or "quota" in str(e).lower():
                raise
    
    return pd.DataFrame(channel_info)


# ======================
# HÀM 4: TẠO DATASET HOÀN CHỈNH
# ======================
def build_youtube_dataset(config, control: ControlManager):
    """
    Xây dựng dataset hoàn chỉnh từ YouTube API dựa trên config từ Control DB
    """
    api_key = config["api_key"]
    search_endpoint = config["search_endpoint"]
    videos_endpoint = config["videos_endpoint"]
    channels_endpoint = config["channels_endpoint"]

    search_queries = parse_config_list(config.get("search_queries"))
    if not search_queries:
        raise ValueError("Vui long cau hinh 'search_queries' cho source YouTube (dang JSON hoac comma separated)")

    max_streams = int(config.get("max_streams", 2000))
    region_code = config.get("region_code", "US")
    video_category_id = config.get("video_category_id")
    search_delay = float(config.get("search_delay_seconds", 1))
    detail_delay = float(config.get("detail_delay_seconds", 0.5))

    # Bước 1.3.1 (Người 1) - Tìm live stream YouTube theo danh sách query
    control.start_step("search_live_streams")
    live_videos = get_youtube_live_streams(
        api_key=api_key,
        search_endpoint=search_endpoint,
        search_queries=search_queries,
        region_code=region_code,
        video_category_id=video_category_id,
        max_results=max_streams,
        delay_seconds=search_delay,
        control=control,
    )
    control.finish_step("search_live_streams", records_processed=len(live_videos))

    if not live_videos:
        print("Khong tim thay live streams nao")
        return pd.DataFrame()

    video_ids = [item.get("id", {}).get("videoId") for item in live_videos if item.get("id", {}).get("videoId")]
    channel_ids = list({item.get("snippet", {}).get("channelId") for item in live_videos if item.get("snippet", {}).get("channelId")})

    print(f"Da lay {len(video_ids)} video IDs va {len(channel_ids)} channel IDs")

    # Bước 1.3.2 (Người 1) - Lấy chi tiết video (statistics, live info)
    control.start_step("fetch_video_details")
    video_details_df = get_video_details(video_ids, api_key, videos_endpoint, detail_delay)
    control.finish_step("fetch_video_details", records_processed=len(video_details_df))
    print(f"Da lay thong tin chi tiet cua {len(video_details_df)} video")

    # Bước 1.3.3 (Người 1) - Lấy thông tin channel mở rộng
    control.start_step("fetch_channel_info")
    channel_info_df = get_channel_info(channel_ids, api_key, channels_endpoint, detail_delay)
    control.finish_step("fetch_channel_info", records_processed=len(channel_info_df))
    print(f"Da lay thong tin cua {len(channel_info_df)} channel")

    # Bước 1.3.4 (Người 1) - Merge toàn bộ dữ liệu video + channel
    control.start_step("merge_dataset")
    videos_df = pd.DataFrame([
        {
            "video_id": item.get("id", {}).get("videoId", ""),
            "channel_id": item.get("snippet", {}).get("channelId", ""),
            "title": item.get("snippet", {}).get("title", ""),
            "channel_title": item.get("snippet", {}).get("channelTitle", ""),
            "published_at": item.get("snippet", {}).get("publishedAt", ""),
            "description": item.get("snippet", {}).get("description", ""),
            "category_id": item.get("snippet", {}).get("categoryId", "")
        }
        for item in live_videos
    ])

    if not video_details_df.empty:
        video_details_clean = pd.DataFrame({
            "video_id": video_details_df["id"],
            "view_count": video_details_df["statistics"].apply(lambda x: x.get("viewCount", 0) if isinstance(x, dict) else 0),
            "like_count": video_details_df["statistics"].apply(lambda x: x.get("likeCount", 0) if isinstance(x, dict) else 0),
            "comment_count": video_details_df["statistics"].apply(lambda x: x.get("commentCount", 0) if isinstance(x, dict) else 0),
            "duration": video_details_df.get("contentDetails", pd.Series(dtype=object)).apply(
                lambda x: x.get("duration", "") if isinstance(x, dict) else ""
            ),
            "live_viewers": video_details_df.get("liveStreamingDetails", pd.Series(dtype=object)).apply(
                lambda x: x.get("concurrentViewers", 0) if isinstance(x, dict) else 0
            )
        })
        videos_df = videos_df.merge(video_details_clean, on="video_id", how="left")

    if not channel_info_df.empty:
        channel_info_clean = pd.DataFrame({
            "channel_id": channel_info_df["id"],
            "subscriber_count": channel_info_df["statistics"].apply(
                lambda x: x.get("subscriberCount", 0) if isinstance(x, dict) else 0
            ),
            "total_videos": channel_info_df["statistics"].apply(
                lambda x: x.get("videoCount", 0) if isinstance(x, dict) else 0
            ),
            "channel_views": channel_info_df["statistics"].apply(
                lambda x: x.get("viewCount", 0) if isinstance(x, dict) else 0
            )
        })
        videos_df = videos_df.merge(channel_info_clean, on="channel_id", how="left")

    viewer_counts = videos_df.get("live_viewers", pd.Series(dtype=object))
    if viewer_counts.empty or viewer_counts.isna().all():
        viewer_counts = videos_df.get("view_count", pd.Series([0] * len(videos_df)))

    # Bước 1.3.5 (Người 1) - Chuẩn hóa schema output trước khi ghi CSV
    df_final = pd.DataFrame({
        "stream_id": videos_df["video_id"].fillna(""),
        "streamer_name": videos_df["channel_title"].fillna(""),
        "platform": "YouTube",
        "game_name": videos_df["title"].fillna("").str[:200],
        "category": videos_df["category_id"].fillna("").astype(str),
        "viewer_count": pd.to_numeric(viewer_counts, errors="coerce").fillna(0).astype(int),
        "follower_count": pd.to_numeric(
            videos_df.get("subscriber_count", pd.Series([0] * len(videos_df))), errors="coerce"
        ).fillna(0).astype(int),
        "language": "",
        "start_time": videos_df["published_at"].fillna(""),
        "capture_time": datetime.now(timezone.utc).isoformat(),
        "stream_title": videos_df["title"].fillna(""),
        "platform_id": videos_df["channel_id"].fillna(""),
        "game_id": videos_df["category_id"].fillna("").astype(str)
    })
    control.finish_step("merge_dataset", records_processed=len(df_final))

    return df_final


def resolve_output_path(config, base_dir):
    default_dir = os.path.join(base_dir, "rawData")
    output_dir = config.get("output_dir", default_dir)
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(base_dir, output_dir)
    os.makedirs(output_dir, exist_ok=True)

    output_file = config.get("output_filename")
    if not output_file:
        raise KeyError("Vui long cau hinh 'output_filename' cho source YouTube trong Control DB")
    return os.path.join(output_dir, output_file)


# ======================
# MAIN (CHẠY ĐỘC LẬP)
# ======================
if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    with ControlManager("extract_youtube_data") as control:
        youtube_config = control.get_source_config(
            "youtube",
            required_keys=[
                "api_key",
                "search_endpoint",
                "videos_endpoint",
                "channels_endpoint",
                "search_queries",
                "output_filename",
            ],
        )

        df_youtube = build_youtube_dataset(youtube_config, control)

        if df_youtube.empty:
            control.log_event("no_data", "Khong co live stream YouTube de luu", {})
            control.mark_job_failed("Khong co du lieu YouTube")
            raise SystemExit("Khong co du lieu YouTube")

        # Bước 1.3.6 (Người 1) - Lưu snapshot YouTube ra CSV và log audit
        output_path = resolve_output_path(youtube_config, BASE_DIR)
        df_youtube.to_csv(output_path, index=False, encoding="utf-8")
        control.log_file(output_path, len(df_youtube), status="loaded")
        control.log_event(
            "file_written",
            "Da ghi file YouTube",
            {"path": output_path, "records": len(df_youtube)}
        )
        print(f"\nFile du lieu YouTube da duoc luu tai: {output_path}")
        print(f"Tong so records: {len(df_youtube)}")
        control.mark_job_completed(records_processed=len(df_youtube))

