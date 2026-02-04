from __future__ import annotations

from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Tuple

from garminconnect import Garmin
from notion_client import Client
from dotenv import load_dotenv
import pytz
import os
import time

# =========================
# Config
# =========================
LOCAL_TZ = pytz.timezone("Asia/Seoul")
API_DELAY_SECONDS = 0.3  # 가민/노션 호출 간 간단 딜레이(너무 빠르면 막힐 수 있음)


# =========================
# Garmin helpers
# =========================
def get_sleep_data_for_date(garmin: Garmin, d: date) -> dict:
    """Garmin expects YYYY-MM-DD"""
    return garmin.get_sleep_data(d.isoformat())


def format_duration(seconds: Optional[int]) -> str:
    minutes = (seconds or 0) // 60
    return f"{minutes // 60}h {minutes % 60}m"


def ts_to_iso_local(timestamp_ms: Optional[int]) -> Optional[str]:
    """Garmin GMT(ms) -> KST ISO8601 string for Notion date."""
    if not timestamp_ms:
        return None
    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt_utc.astimezone(LOCAL_TZ).isoformat()


def ts_to_hhmm_local(timestamp_ms: Optional[int]) -> str:
    """Garmin GMT(ms) -> KST HH:MM string."""
    if not timestamp_ms:
        return "Unknown"
    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt_utc.astimezone(LOCAL_TZ).strftime("%H:%M")


# =========================
# Notion fetch (by Date range)
# =========================
def notion_fetch_existing_pages_by_date(
    client: Client,
    database_id: str,
    start_date: str,
    end_date: str,
) -> Dict[str, List[dict]]:
    """
    Fetch pages in [start_date, end_date] (inclusive).
    Return mapping: { "YYYY-MM-DD": [ {page_id, resting_hr}, ... ] }
    """
    by_date: Dict[str, List[dict]] = {}
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
            rhr = props.get("Resting HR", {}).get("number", 0) or 0

            by_date.setdefault(day, []).append(
                {"page_id": page["id"], "resting_hr": rhr}
            )

        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")

    return by_date


# =========================
# Build Notion properties from Garmin
# =========================
def build_sleep_properties_for_target_date(
    sleep_data: dict,
    target_date_str: str,
    skip_zero_sleep: bool = True,
) -> Optional[dict]:
    """
    ✅ 핵심: Notion 'Date'는 무조건 target_date_str(루프 날짜)로 고정.
    Garmin calendarDate가 다르게 와도 Notion엔 target_date로 저장/업데이트.
    """
    daily_sleep = sleep_data.get("dailySleepDTO", {})
    if not daily_sleep:
        return None

    start_ts = daily_sleep.get("sleepStartTimestampGMT")
    end_ts = daily_sleep.get("sleepEndTimestampGMT")
    if not start_ts or not end_ts:
        return None

    total_sleep = sum(
        (daily_sleep.get(k, 0) or 0)
        for k in ["deepSleepSeconds", "lightSleepSeconds", "remSleepSeconds"]
    )
    if skip_zero_sleep and total_sleep == 0:
        return None

    times_title = f"{ts_to_hhmm_local(start_ts)} → {ts_to_hhmm_local(end_ts)}"

    props = {
        "Times": {"title": [{"text": {"content": times_title}}]},
        "Date": {"date": {"start": target_date_str}},

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

    # Resting HR: defensive
    rhr = sleep_data.get("restingHeartRate")
    if rhr is None:
        rhr = daily_sleep.get("restingHeartRate")
    props["Resting HR"] = {"number": (rhr or 0)}

    # Full Date/Time range (optional)
    start_iso = ts_to_iso_local(start_ts)
    end_iso = ts_to_iso_local(end_ts)
    if start_iso:
        props["Full Date/Time"] = {"date": {"start": start_iso}}
        if end_iso:
            props["Full Date/Time"]["date"]["end"] = end_iso

    return props


# =========================
# Notion write ops
# =========================
def notion_create_sleep_page(
    client: Client,
    database_id: str,
    properties: dict,
) -> None:
    client.pages.create(
        parent={"database_id": database_id},
        properties=properties,
        icon={"emoji": "😴"},
    )


def notion_update_sleep_page(
    client: Client,
    page_id: str,
    properties: dict,
) -> None:
    client.pages.update(page_id=page_id, properties=properties)


# =========================
# Main sync logic (your exact rules)
# =========================
def sync_sleep_range_last_n_days(n_days: int = 30, skip_zero_sleep: bool = True) -> None:
    load_dotenv()

    garmin_email = os.getenv("GARMIN_EMAIL")
    garmin_password = os.getenv("GARMIN_PASSWORD")
    notion_token = os.getenv("NOTION_TOKEN")
    database_id = os.getenv("NOTION_SLEEP_DB_ID")

    if not all([garmin_email, garmin_password, notion_token, database_id]):
        raise ValueError("Missing env vars: GARMIN_EMAIL, GARMIN_PASSWORD, NOTION_TOKEN, NOTION_SLEEP_DB_ID")

    # Login
    garmin = Garmin(garmin_email, garmin_password)
    garmin.login()
    client = Client(auth=notion_token)

    today_kst = datetime.now(LOCAL_TZ).date()
    start_day = today_kst - timedelta(days=n_days - 1)
    start_str = start_day.isoformat()
    end_str = today_kst.isoformat()

    # Fetch existing Notion pages in range
    existing_by_date = notion_fetch_existing_pages_by_date(client, database_id, start_str, end_str)
    print(f"[Notion] Existing dates in range: {len(existing_by_date)} (pages total: {sum(len(v) for v in existing_by_date.values())})")

    created = 0
    updated = 0
    skipped = 0
    errors = 0
    duplicate_dates = 0

    for i in range(n_days):
        d = start_day + timedelta(days=i)
        d_str = d.isoformat()

        pages = existing_by_date.get(d_str, [])

        # (선택) 중복 감지 로그
        if len(pages) > 1:
            duplicate_dates += 1

        # ===== Rule 3: 해당 날짜 있고 Resting HR > 0 이면 아무 것도 하지마
        if pages and any(p["resting_hr"] > 0 for p in pages):
            skipped += 1
            continue

        # ===== Rule 1/2: 없거나(0개) / 있는데 전부 HR=0이면 -> Garmin에서 해당 날짜 수면 가져와서 create/update
        try:
            time.sleep(API_DELAY_SECONDS)
            sleep_data = get_sleep_data_for_date(garmin, d)
        except Exception as e:
            print(f"[Garmin] Error {d_str}: {e}")
            errors += 1
            continue

        props = build_sleep_properties_for_target_date(sleep_data, target_date_str=d_str, skip_zero_sleep=skip_zero_sleep)
        if not props:
            # 수면 데이터가 없거나(혹은 0 sleep) 스킵
            skipped += 1
            continue

        try:
            time.sleep(API_DELAY_SECONDS)
            if not pages:
                # ===== Rule 1: 노션에 해당 날짜 없어? 만들고 입력
                notion_create_sleep_page(client, database_id, props)
                created += 1
                times_title = props["Times"]["title"][0]["text"]["content"]
                print(f"[CREATE] {d_str} ({times_title})")

            else:
                # ===== Rule 2: 해당 날짜 있고 Resting HR=0 -> 덮어쓰기 업데이트
                # 중복이 있으면 HR=0인 페이지들 전부 업데이트 (안전하게)
                for p in pages:
                    notion_update_sleep_page(client, p["page_id"], props)
                    updated += 1
                times_title = props["Times"]["title"][0]["text"]["content"]
                if len(pages) == 1:
                    print(f"[UPDATE] {d_str} ({times_title})")
                else:
                    print(f"[UPDATE x{len(pages)}] {d_str} ({times_title})  <-- duplicate date pages")

        except Exception as e:
            print(f"[Notion] Write error {d_str}: {e}")
            errors += 1
            continue

    print("\n==== Done ====")
    print(f"Created: {created}")
    print(f"Updated: {updated} (pages updated)")
    print(f"Skipped: {skipped}")
    print(f"Errors:  {errors}")
    print(f"Duplicate dates detected: {duplicate_dates}")


def main():
    sync_sleep_range_last_n_days(n_days=30, skip_zero_sleep=True)


if __name__ == "__main__":
    main()
