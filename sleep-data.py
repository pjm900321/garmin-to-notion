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
    """Notion date expects ISO8601. Convert Garmin GMT ms -> KST ISO."""
    if not timestamp_ms:
        return None
    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    dt_local = dt_utc.astimezone(LOCAL_TZ)
    return dt_local.isoformat()


def ts_to_hhmm_local(timestamp_ms):
    if not timestamp_ms:
        return "Unknown"
    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt_utc.astimezone(LOCAL_TZ).strftime("%H:%M")


def format_date_korean(sleep_date_str: str):
    # sleep_date_str: "YYYY-MM-DD"
    if not sleep_date_str:
        return "Unknown"
    d = datetime.strptime(sleep_date_str, "%Y-%m-%d").date()
    return f"{d.year}년 {d.month}월 {d.day}일"


def notion_fetch_existing_dates(client: Client, database_id: str, start_date: str, end_date: str):
    """
    Fetch all existing pages in the DB whose 'Long Date' is within [start_date, end_date]
    Returns a set of 'YYYY-MM-DD' strings.
    """
    existing = set()
    start_cursor = None

    while True:
        resp = client.databases.query(
            database_id=database_id,
            start_cursor=start_cursor,
            filter={
                "and": [
                    {"property": "Long Date", "date": {"on_or_after": start_date}},
                    {"property": "Long Date", "date": {"on_or_before": end_date}},
                ]
            }
        )

        for page in resp.get("results", []):
            props = page.get("properties", {})
            long_date = props.get("Long Date", {}).get("date", {})
            if long_date and long_date.get("start"):
                # "YYYY-MM-DD" or ISO date-time; we only keep date part
                existing.add(long_date["start"][:10])

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

    total_sleep = sum((daily_sleep.get(k, 0) or 0) for k in ["deepSleepSeconds", "lightSleepSeconds", "remSleepSeconds"])

    if skip_zero_sleep and total_sleep == 0:
        print(f"Skipping sleep data for {sleep_date} (total sleep = 0)")
        return

    start_ts = daily_sleep.get("sleepStartTimestampGMT")
    end_ts = daily_sleep.get("sleepEndTimestampGMT")

    properties = {
        # ✅ Title: "YYYY년 M월 D일"
        "Date": {"title": [{"text": {"content": format_date_korean(sleep_date)}}]},

        # ✅ Readable Times in KST
        "Times": {
            "rich_text": [
                {"text": {"content": f"{ts_to_hhmm_local(start_ts)} → {ts_to_hhmm_local(end_ts)}"}}
            ]
        },

        # ✅ Date key (range queries rely on this)
        "Long Date": {"date": {"start": sleep_date}},

        # ✅ Store full date-time (KST ISO)
        "Full Date/Time": {"date": {"start": ts_to_iso_local(start_ts), "end": ts_to_iso_local(end_ts)}},

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

        "Resting HR": {"number": sleep_data.get("restingHeartRate", 0) or 0},
    }

    client.pages.create(parent={"database_id": database_id}, properties=properties, icon={"emoji": "😴"})
    print(f"Created sleep entry for: {sleep_date}")


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
    start_day = today_kst - timedelta(days=364)  # today 포함 365일

    start_str = start_day.isoformat()
    end_str = today_kst.isoformat()

    # ✅ Notion에 이미 존재하는 날짜를 범위로 한 번에 로딩
    existing_dates = notion_fetch_existing_dates(client, database_id, start_str, end_str)
    print(f"Existing notion entries in range: {len(existing_dates)}")

    created = 0
    for i in range(365):
        d = start_day + timedelta(days=i)
        d_str = d.isoformat()

        if d_str in existing_dates:
            continue

        data = get_sleep_data_for_date(garmin, d)
        if data:
            create_sleep_data(client, database_id, data, skip_zero_sleep=True)
            created += 1

    print(f"Done. Created: {created} entries (missing days only).")


if __name__ == "__main__":
    main()
