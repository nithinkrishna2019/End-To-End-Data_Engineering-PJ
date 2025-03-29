import json
import random
import boto3
import uuid
from datetime import datetime, timezone, timedelta

IST_OFFSET = timedelta(hours=5, minutes=30)

# Initialize S3 client
s3 = boto3.client("s3")

# S3 bucket name
BUCKET_NAME = "aws-glue-s3-bucket"

def get_weather_conditions(temp, humidity):
    """Return realistic weather conditions based on temperature and humidity."""
    if temp > 35:
        return "Hot" if humidity < 50 else "Humid"
    elif 30 <= temp <= 35:
        return "Sunny" if humidity < 60 else "Partly Cloudy"
    elif 20 <= temp < 30:
        return "Cloudy" if humidity > 70 else "Clear"
    elif 15 <= temp < 20:
        return "Foggy" if humidity > 80 else "Cool"
    elif 10 <= temp < 15:
        return "Chilly"
    else:
        return "Cold"

def generate_weather_data():
    cities_data = {
        "Mumbai": {"avg_temp": 30, "humidity_range": (60, 90), "monsoon": True},
        "Delhi": {"avg_temp": 25, "humidity_range": (30, 60), "monsoon": False},
        "Bangalore": {"avg_temp": 22, "humidity_range": (50, 80), "monsoon": True},
        "Kolkata": {"avg_temp": 28, "humidity_range": (60, 85), "monsoon": True},
        "Chennai": {"avg_temp": 32, "humidity_range": (55, 80), "monsoon": True},
        "Hyderabad": {"avg_temp": 27, "humidity_range": (40, 70), "monsoon": False},
        "Pune": {"avg_temp": 26, "humidity_range": (50, 75), "monsoon": True},
        "Ahmedabad": {"avg_temp": 30, "humidity_range": (30, 60), "monsoon": False}
    }

    weather_data = []

    for city, city_info in cities_data.items():
        temp = round(random.uniform(city_info["avg_temp"] - 5, city_info["avg_temp"] + 5), 1)
        humidity = random.randint(city_info["humidity_range"][0], city_info["humidity_range"][1])

        # Ensure rain only happens in monsoon-prone cities and with high humidity
        if city_info["monsoon"] and humidity > 75:
            conditions = "Rainy"
        else:
            conditions = get_weather_conditions(temp, humidity)

        data = {
            "ID": str(uuid.uuid4()),
            "city": city,
            "country": "India",
            "timestamp": (datetime.now(timezone.utc) + IST_OFFSET).isoformat(),
            "weather": {
                "temperature_celsius": temp,
                "humidity_percent": humidity,
                "pressure_hpa": random.randint(990, 1030),
                "wind_speed_kph": round(random.uniform(0, 20), 1),
                "wind_direction": random.choice(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]),
                "conditions": conditions
            }
        }

        weather_data.append(data)

    # Convert data to JSON
    json_data = json.dumps(weather_data, indent=4)

    ist_now = datetime.now(timezone.utc) + IST_OFFSET
    filename=f"weather_{ist_now.strftime('%Y-%m-%d_%H-%M-%S')}_IST.json"
    path = f"End-to-End-PJ/source-json-data/{filename}"

    # Upload to S3
    s3.put_object(Bucket=BUCKET_NAME, Key=path, Body=json_data, ContentType="application/json")

    print(f"Uploaded {filename} to S3")

if __name__ == "__main__":
    generate_weather_data()
