import os
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")

try:
    client = MongoClient(MONGO_DB_URL, tlsCAFile=certifi.where())
    client.admin.command("ping")
    print("✅ MongoDB connected successfully!")
except Exception as e:
    print("❌ MongoDB connection failed")
    print(e)
