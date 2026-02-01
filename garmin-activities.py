from datetime import datetime, timezone
from garminconnect import Garmin
from notion_client import Client
from dotenv import load_dotenv
import pytz
import os

# Your local time zone, replace with the appropriate one if needed
local_tz = pytz.timezone('America/Toronto')

ACTIVITY_ICONS = {
    "Barre": "https://img.icons8.com/?size=100&id=66924&format=png&color=000000",
    "Breathwork": "https://img.icons8.com/?size=100&id=9798&format=png&color=000000",
    "Cardio": "https://img.icons8.com/?size=100&id=71221&format=png&color=000000",
    "Cycling": "https://img.icons8.com/?size=100&id=47443&format=png&color=000000",
    "Hiking": "https://img.icons8.com/?size=100&id=9844&format=png&color=000000",
    "Indoor Cardio": "https://img.icons8.com/?size=100&id=62779&format=png&color=000000",
    "Indoor Cycling": "https://img.icons8.com/?size=100&id=47443&format=png&color=000000",
    "Indoor Rowing": "https://img.icons8.com/?size=100&id=71098&format=png&color=000000",
    "Pilates": "https://img.icons8.com/?size=100&id=9774&format=png&color=000000",
    "Meditation": "https://img.icons8.com/?size=100&id=9798&format=png&color=000000",
    "Rowing": "https://img.icons8.com/?size=100&id=71491&format=png&color=000000",
    "Running": "https://img.icons8.com/?size=100&id=k1l1XFkME39t&format=png&color=000000",
    "Strength Training": "https://img.icons8.com/?size=100&id=107640&format=png&color=000000",
    "Stretching": "https://img.icons8.com/?size=100&id=djfOcRn1m_kh&format=png&color=000000",
    "Swimming": "https://img.icons8.com/?size=100&id=9777&format=png&color=000000",
    "Treadmill Running": "https://img.icons8.com/?size=100&id=9794&format=png&color=000000",
    "Walking": "https://img.icons8.com/?size=100&id=9807&format=png&color=000000",
    "Yoga": "https://img.icons8.com/?size=100&id=9783&format=png&color=000000",
    # Add more mappings as needed
}

def get_all_activities(garmin, limit=1000):
    return garmin.get_activities(0, limit)

def format_activity_type(activity_type, activity_name=""):
    # First format the activity type as before
    formatted_type = activity_type.replace('_', ' ').title() if activity_type else "Unknown"

    # Initialize subtype as the same as the main type
    activity_subtype = formatted_type
    activity_type = formatted_type

    # Map of specific subtypes to their main types
    activity_mapping = {
        "Barre": "Strength",
        "Indoor Cardio": "Cardio",
        "Indoor Cycling": "Cycling",
        "Indoor Rowing": "Rowing",
        "Speed Walking": "Walking",
        "Strength Training": "Strength",
        "Treadmill Running": "Running"
    }

    # Special replacement for Rowing V2
    if formatted_type == "Rowing V2":
        activity_type = "Rowing"

    # Special case for Yoga and Pilates
    elif formatted_type in ["Yoga", "Pilates"]:
        activity_type = "Yoga/Pilates"
        activity_subtype = formatted_type

    # If the formatted type is in our mapping, update both main type and subtype
    if formatted_type in activity_mapping:
        activity_type = activity_mapping[formatted_type]
        activity_subtype = formatted_type

    # Special cases for activity names
    if activity_name and "meditation" in activity_name.lower():
        return "Meditation", "Meditation"
    if activity_name and "barre" in activity_name.lower():
        return "Strength", "Barre"
    if activity_name and "stretch" in activity_name.lower():
        return "Stretching", "Stretching"
    
    return activity_type, activity_subtype

def format_entertainment(activity_name):
    return activity_name.replace('ENTERTAINMENT', 'Netflix')

def format_training_message(message):
    messages = {
        'NO_': 'No Benefit',
        'MINOR_': 'Some Benefit',
        'RECOVERY_': 'Recovery',
        'MAINTAINING_': 'Maintaining',
        'IMPROVING_': 'Impacting',
        'IMPACTING_': 'Impacting',
        'HIGHLY_': 'Highly Impacting',
        'OVERREACHING_': 'Overreaching'
    }
    for key, value in messages.items():
        if message.startswith(key):
            return value
    return message

def format_training_effect(trainingEffect_label):
    return trainingEffect_label.replace('_', ' ').title()

def format_pace(average_speed):
    if average_speed > 0:
        pace_min_km = 1000 / (average_speed * 60)  # Convert to min/km
        minutes = int(pace_min_km)
        seconds = int((pace_min_km - minutes) * 60)
        return f"{minutes}:{seconds:02d} min/km"
    else:
        return ""
    
def activity_exists(client, database_id, activity_date):
    """시작 시간으로 중복 체크 (같은 시간에 두 운동 불가능)"""
    try:
        query = client.databases.query(
            database_id=database_id,
            filter={"property": "Date", "date": {"equals": activity_date}}
        )
        results = query['results']
        return results[0] if results else None
    except Exception as e:
        print(f"Error querying Notion: {e}")
        return None


def create_activity(client, database_id, activity):

    # Create a new activity in the Notion database
    activity_date = activity.get('startTimeGMT')
    activity_name = format_entertainment(activity.get('activityName', 'Unnamed Activity'))
    activity_type, activity_subtype = format_activity_type(
        activity.get('activityType', {}).get('typeKey', 'Unknown'),
        activity_name
    )
    
    # Get icon for the activity type
    icon_url = ACTIVITY_ICONS.get(activity_subtype if activity_subtype != activity_type else activity_type)
    
    properties = {
        "Date": {"date": {"start": activity_date}},
        "Activity Type": {"select": {"name": activity_type}},
        "Subactivity Type": {"select": {"name": activity_subtype}},
        "Activity Name": {"title": [{"text": {"content": activity_name}}]},
        "Distance (km)": {"number": round(activity.get('distance', 0) / 1000, 2)},
        "Duration (min)": {"number": round(activity.get('duration', 0) / 60, 2)},
        "Calories": {"number": round(activity.get('calories', 0))},
        "Avg Pace": {"rich_text": [{"text": {"content": format_pace(activity.get('averageSpeed', 0))}}]},
        "Avg Power": {"number": round(activity.get('avgPower', 0), 1)},
        "Max Power": {"number": round(activity.get('maxPower', 0), 1)},
        "Training Effect": {"select": {"name": format_training_effect(activity.get('trainingEffectLabel', 'Unknown'))}},
        "Aerobic": {"number": round(activity.get('aerobicTrainingEffect', 0), 1)},
        "Aerobic Effect": {"select": {"name": format_training_message(activity.get('aerobicTrainingEffectMessage', 'Unknown'))}},
        "Anaerobic": {"number": round(activity.get('anaerobicTrainingEffect', 0), 1)},
        "Anaerobic Effect": {"select": {"name": format_training_message(activity.get('anaerobicTrainingEffectMessage', 'Unknown'))}},
        "PR": {"checkbox": activity.get('pr', False)},
        "Fav": {"checkbox": activity.get('favorite', False)}
    }
    
    page = {
        "parent": {"database_id": database_id},
        "properties": properties,
    }
    
    if icon_url:
        page["icon"] = {"type": "external", "external": {"url": icon_url}}
    
    try:
        client.pages.create(**page)
        print(f"Created: {activity_name}")
    except Exception as e:
        print(f"Error creating activity: {e}")


def main():
    load_dotenv()

    # Initialize Garmin and Notion clients using environment variables
    garmin_email = os.getenv("GARMIN_EMAIL")
    garmin_password = os.getenv("GARMIN_PASSWORD")
    notion_token = os.getenv("NOTION_TOKEN")
    database_id = os.getenv("NOTION_DB_ID")

    # Initialize Garmin client and login
    garmin = Garmin(garmin_email, garmin_password)
    garmin.login()
    client = Client(auth=notion_token)
    
    # Get all activities
    activities = get_all_activities(garmin)

    # Process all activities
    for activity in activities:
        activity_date = activity.get('startTimeGMT')
        
        # 시작 시간으로만 중복 체크 (없으면 생성, 있으면 패스)
        if not activity_exists(client, database_id, activity_date):
            create_activity(client, database_id, activity)

    print("Activity sync completed!")


if __name__ == '__main__':
    main()
