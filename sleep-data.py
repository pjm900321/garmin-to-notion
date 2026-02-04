from datetime import datetime, timedelta, timezone, date
from garminconnect import Garmin
from notion_client import Client
from dotenv import load_dotenv
import pytz
import os

# ✅ Korea timezone
LOCAL_TZ = pytz.timezone("Asia/Seoul")


def get_sleep_data_for_date(garmin: Garmin, d: date):
    # Garmin expects YYYY-MM-DD
    return garmin.get_sleep_data(d.isoformat())


def format_duration(seconds):
    minutes = (seconds or 0) // 60
    return f"{minutes // 60}h {minutes % 60}m"


def ts_to_iso_local(timestamp_ms):
    """Convert Garmin GMT(ms) -> KST ISO8601 string for Notion date."""
    if not timestamp_ms:
        return None
    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt_utc.astimezone(LOCAL_TZ).isoformat()


def ts_to_hhmm_local(timestamp_ms):
    """Convert Garmin GMT(ms) -> KST HH:MM string."""
    if not timestamp_ms:
        return "Unknown"
    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt_utc.astimezone(LOCAL_TZ).strftime("%H:%M")


def notion_fetch_existing_pages(client: Client, database_id: str, start_date: str, end_date: str):
    """
    Fetch all existing pages in the DB whose 'Date' is within [start_date, end_date]
    Returns dict:
    {
        "YYYY-MM-DD": {
            "page_id": "...",
            "resting_hr": 0
        }
    }
    """
    existing = {}
    start_cursor = None

    while True:
        resp = client.databases.query(
            database_id=database_id,
            start_cursor=start_cursor,
            page_size=100,
            filter={
                "and": [
                    {"property": "Date", "date": {"on_or_after": start_date}},
                    {"property": "Date", "date": {"on_or_before": end_date}},
                ]
            },
        )

        for page in resp.get("results", []):
            props = page.get("properties", {})
            d = props.get("Date", {}).get("date", {})
            if not d or not d.get("start"):
                continue

            day = d["start"][:10]
            rhr = props.get("Resting HR", {}).get("number", 0)

            existing[day] = {"page_id": page["id"], "resting_hr": (rhr or 0)}

        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")

    return existing


def create_sleep_data(client: Client, database_id: str, sleep_data: dict, skip_zero_sleep: bool = True):
    daily_sleep = sleep_data.get("dailySleepDTO", {})
    if not daily_sleep:
        return

    sleep_date = daily_sleep.get("calendarDate")  # "YYYY-MM-DD"
    if not sleep_date:
        return

    start_ts = daily_sleep.get("sleepStartTimestampGMT")
    end_ts = daily_sleep.get("sleepEndTimestampGMT")

    # ✅ Garmin이 수면 없는데도 0으로 리턴하는 케이스 방지 (Unknown/0h 생성 방지)
    if not start_ts or not end_ts:
        return

    total_sleep = sum(
        (daily_sleep.get(k, 0) or 0) for k in ["deepSleepSeconds", "lightSleepSeconds", "remSleepSeconds"]
    )

    if skip_zero_sleep and total_sleep == 0:
        print(f"Skipping sleep data for {sleep_date} (total sleep = 0)")
        return

    # ✅ Title should be Times
    times_title = f"{ts_to_hhmm_local(start_ts)} → {ts_to_hhmm_local(end_ts)}"

    properties = {
        # ✅ Times is TITLE (required)
        "Times": {"title": [{"text": {"content": times_title}}]},

        # ✅ Date is DATE (key for range queries & dedupe)
        "Date": {"date": {"start": sleep_date}},

        "Total Sleep (h)": {"number": round(total_sleep / 3600, 1)},
        "Light Sleep (h)": {"number": round((daily_sleep.get("lightSleepSeconds", 0) or 0) / 3600, 1)},
        "Deep Sleep (h)": {"number": round((daily_sleep.get("deepSleepSeconds", 0) or 0) / 3600, 1)},
        "REM Sleep (h)": {"number": round((daily_sleep.get("remSleepSeconds", 0) or 0) / 3600, 1)},
        "Awake Time (h)": {"number": round((daily_sleep.get("awakeSleepSeconds", 0) or 0) / 3600, 1)},

        "Total Sleep": {"rich_text": [{"text": {"content": format_duration(total_sleep)}}]},
        "Light Sleep": {"rich_text": [{"text": {"content": format_duration(daily_sleep.get("lightSleepSeconds", 0))}}]},
        "Deep Sleep": {"rich_text": [{"text": {"content": format_duration(daily_sleep.get("deepSleepSeconds", 0))}}]},
        "REM Sleep": {"rich_text": [{"text": {"content": format_duration(daily_sleep.get("remSleepSeconds", 0))}}]},
        "Awake Time": {"rich_text": [{"text": {"content": format_duration(daily_sleep.get("awakeSleepSeconds", 0))}}]},
    }

    # Resting HR can appear in different places; be defensive
    rhr = sleep_data.get("restingHeartRate")
    if rhr is None:
        rhr = daily_sleep.get("restingHeartRate")
    properties["Resting HR"] = {"number": (rhr or 0)}

    # Full Date/Time: avoid sending None values
    start_iso = ts_to_iso_local(start_ts)
    end_iso = ts_to_iso_local(end_ts)

    if start_iso:
        properties["Full Date/Time"] = {"date": {"start": start_iso}}
        if end_iso:
            properties["Full Date/Time"]["date"]["end"] = end_iso

    client.pages.create(parent={"database_id": database_id}, properties=properties, icon={"emoji": "😴"})
    print(f"Created sleep entry for: {sleep_date} ({times_title})")


def update_sleep_data(client: Client, page_id: str, sleep_data: dict):
    """
    Update existing Notion page when Resting HR was 0.
    + Garmin '가짜 0' DTO 방지 (start/end 없으면 업데이트 안 함)
    """
    daily_sleep = sleep_data.get("dailySleepDTO", {})
    if not daily_sleep:
        return

    start_ts = daily_sleep.get("sleepStartTimestampGMT")
    end_ts = daily_sleep.get("sleepEndTimestampGMT")
    if not start_ts or not end_ts:
        return

    rhr = sleep_data.get("restingHeartRate")
    if rhr is None:
        rhr = daily_sleep.get("restingHeartRate")
    rhr = rhr or 0

    client.pages.update(
        page_id=page_id,
        properties={
            "Resting HR": {"number": rhr},
        },
    )


def main():
    load_dotenv()

    garmin_email = os.getenv("GARMIN_EMAIL")
    garmin_password = os.getenv("GARMIN_PASSWORD")
    notion_token = os.getenv("NOTION_TOKEN")
    database_id = os.getenv("NOTION_SLEEP_DB_ID")

    if not all([garmin_email, garmin_password, notion_token, database_id]):
        raise ValueError("Missing env vars: GARMIN_EMAIL, GARMIN_PASSWORD, NOTION_TOKEN, NOTION_SLEEP_DB_ID")

    garmin = Garmin(garmin_email, garmin_password)
    garmin.login()

    client = Client(auth=notion_token)

    # ✅ Today 기준을 한국시간으로
    today_kst = datetime.now(LOCAL_TZ).date()

    # ✅ 최근 30일만 (오늘 포함)
    start_day = today_kst - timedelta(days=29)

    start_str = start_day.isoformat()
    end_str = today_kst.isoformat()

    # ✅ Notion에 이미 존재하는 페이지(Date 범위) 로딩 (page_id + resting_hr)
    existing_pages = notion_fetch_existing_pages(client, database_id, start_str, end_str)
    print(f"Existing notion entries in range: {len(existing_pages)}")

    created = 0
    updated = 0

    for i in range(30):
        d = start_day + timedelta(days=i)
        d_str = d.isoformat()

        page_info = existing_pages.get(d_str)

        # ✅ 이미 있고 Resting HR > 0 이면 스킵
        if page_info and page_info["resting_hr"] > 0:
            continue

        # ❗ 없거나(Rest HR=0 포함) Garmin 재조회
        try:
            data = get_sleep_data_for_date(garmin, d)
        except Exception as e:
            print(f"Garmin error {d_str}: {e}")
            continue

        if not data:
            continue

        if page_info:
            # 🔁 UPDATE (Resting HR=0 케이스)
            update_sleep_data(client, page_info["page_id"], data)
            updated += 1
            print(f"Updated Resting HR for: {d_str}")
        else:
            # ➕ CREATE
            create_sleep_data(client, database_id, data, skip_zero_sleep=True)
            created += 1

    print(f"Done. Created: {created} entries. Updated: {updated} entries (Resting HR=0).")


if __name__ == "__main__":
    main()
