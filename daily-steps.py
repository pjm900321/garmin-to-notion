from datetime import date, timedelta
from garminconnect import Garmin
from notion_client import Client
from dotenv import load_dotenv
import os
import time

# Constants
API_DELAY_SECONDS = 1  # Rate limiting 방지


def get_daily_steps_for_date(garmin, d):
    """특정 날짜의 걸음 수 데이터 가져오기"""
    try:
        steps_data = garmin.get_daily_steps(d.isoformat(), d.isoformat())
        return steps_data[0] if steps_data else None
    except Exception as e:
        print(f"Error fetching steps data for {d}: {e}")
        return None


def daily_steps_exist(client, database_id, activity_date):
    """Notion 데이터베이스에 해당 날짜의 걸음 수 데이터가 있는지 확인"""
    try:
        query = client.databases.query(
            database_id=database_id,
            filter={
                "and": [
                    {"property": "Date", "date": {"equals": activity_date}},
                    {"property": "Activity Type", "title": {"equals": "Walking"}}
                ]
            }
        )
        results = query['results']
        return results[0] if results else None
    except Exception as e:
        print(f"Error querying Notion for {activity_date}: {e}")
        return None


def steps_need_update(existing_steps, new_steps):
    """기존 데이터와 새 데이터 비교하여 업데이트 필요 여부 확인"""
    existing_props = existing_steps['properties']
    
    new_total_steps = new_steps.get('totalSteps') or 0
    new_step_goal = new_steps.get('stepGoal') or 0
    new_total_distance = new_steps.get('totalDistance') or 0
    
    return (
        existing_props['Total Steps']['number'] != new_total_steps or
        existing_props['Step Goal']['number'] != new_step_goal or
        existing_props['Total Distance (km)']['number'] != round(new_total_distance / 1000, 2)
    )


def update_daily_steps(client, existing_steps, new_steps):
    """기존 걸음 수 데이터 업데이트"""
    total_distance = new_steps.get('totalDistance') or 0
    
    properties = {
        "Activity Type": {"title": [{"text": {"content": "Walking"}}]},
        "Total Steps": {"number": new_steps.get('totalSteps') or 0},
        "Step Goal": {"number": new_steps.get('stepGoal') or 0},
        "Total Distance (km)": {"number": round(total_distance / 1000, 2)}
    }
    
    try:
        client.pages.update(page_id=existing_steps['id'], properties=properties)
        print(f"Updated steps for: {new_steps.get('calendarDate')}")
    except Exception as e:
        print(f"Error updating steps for {new_steps.get('calendarDate')}: {e}")


def create_daily_steps(client, database_id, steps, skip_zero_steps=True):
    """새 걸음 수 데이터 생성"""
    total_steps = steps.get('totalSteps') or 0
    steps_date = steps.get('calendarDate')
    
    if skip_zero_steps and total_steps == 0:
        print(f"Skipping steps data for {steps_date} as total steps is 0")
        return
    
    total_distance = steps.get('totalDistance') or 0
    
    properties = {
        "Activity Type": {"title": [{"text": {"content": "Walking"}}]},
        "Date": {"date": {"start": steps_date}},
        "Total Steps": {"number": total_steps},
        "Step Goal": {"number": steps.get('stepGoal') or 0},
        "Total Distance (km)": {"number": round(total_distance / 1000, 2)}
    }
    
    page = {
        "parent": {"database_id": database_id},
        "properties": properties,
    }
    
    try:
        client.pages.create(**page)
        print(f"Created steps entry for: {steps_date}")
    except Exception as e:
        print(f"Error creating steps for {steps_date}: {e}")


def main():
    load_dotenv()

    garmin_email = os.getenv("GARMIN_EMAIL")
    garmin_password = os.getenv("GARMIN_PASSWORD")
    notion_token = os.getenv("NOTION_TOKEN")
    database_id = os.getenv("NOTION_STEPS_DB_ID")

    # 필수 환경 변수 확인
    if not all([garmin_email, garmin_password, notion_token, database_id]):
        print("Error: Missing required environment variables")
        return

    try:
        garmin = Garmin(garmin_email, garmin_password)
        garmin.login()
    except Exception as e:
        print(f"Garmin login failed: {e}")
        return

    client = Client(auth=notion_token)

    # 항상 365일치 동기화
    lookback_days = 365

    today = date.today()
    start_date = today - timedelta(days=lookback_days)

    print(f"Syncing steps data from {start_date} to {today - timedelta(days=1)}")

    d = start_date
    while d < today:
        steps_data = get_daily_steps_for_date(garmin, d)
        
        if steps_data:
            steps_date = steps_data.get('calendarDate')
            existing_steps = daily_steps_exist(client, database_id, steps_date)
            
            if existing_steps:
                if steps_need_update(existing_steps, steps_data):
                    update_daily_steps(client, existing_steps, steps_data)
            else:
                create_daily_steps(client, database_id, steps_data, skip_zero_steps=True)
        
        time.sleep(API_DELAY_SECONDS)
        d += timedelta(days=1)

    print("Steps sync completed!")


if __name__ == '__main__':
    main()
