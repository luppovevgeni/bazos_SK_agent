import pymongo
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")

client = pymongo.MongoClient("mongodb://admin:FT7C69hX4O2239a@185.185.142.51:27017/?authSource=admin", tz_aware=True, tzinfo=MSK)

db = client["bazos_SK"]

settings = db["settings"]
profiles = db["profiles"]
advertisements = db["advertisements"]
agents = db["agents"]
logs = db["logs"]