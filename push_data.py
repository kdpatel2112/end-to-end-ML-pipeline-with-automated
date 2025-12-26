import os
import sys
import json
import pandas as pd
import pymongo
import certifi
from dotenv import load_dotenv

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
ca = certifi.where()


class NetworkDataExtract:
    def __init__(self):
        try:
            self.mongo_client = pymongo.MongoClient(
                MONGO_DB_URL,
                tlsCAFile=ca
            )
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json_convertor(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = json.loads(data.to_json(orient="records"))
            return data, records   # 🔥 return BOTH
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        try:
            db = self.mongo_client[database]
            col = db[collection]
            col.insert_many(records)
            logging.info(f"Inserted {len(records)} records into MongoDB")
            return len(records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)


if __name__ == "__main__":
    FILE_PATH = r"Network_Data\phisingData.csv"
    DATABASE = "KRISHANA"
    COLLECTION = "NetworkData"

    network_obj = NetworkDataExtract()

    data, records = network_obj.csv_to_json_convertor(FILE_PATH)

    print(f"Total Records: {len(records)}")
    print("\nSample Data (first 5 rows):")
    print(data.head())   # ✅ NOW IT WORKS

    inserted = network_obj.insert_data_mongodb(
        records, DATABASE, COLLECTION
    )

    print(f"Inserted Records: {inserted}")
