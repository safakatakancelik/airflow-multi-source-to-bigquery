"""Load seed/mongo_playlists.json into MongoDB Atlas (replaces the collection; safe to re-run).

MONGO_URI='mongodb+srv://user:pass@cluster/...' python seed/load_mongo.py
"""

import json
import os
from pathlib import Path

from pymongo import MongoClient

uri = os.environ["MONGO_URI"]
database = os.environ.get("MONGO_DATABASE", "ingest_demo")
docs = json.loads((Path(__file__).parent / "mongo_playlists.json").read_text())

collection = MongoClient(uri)[database]["playlists"]
collection.delete_many({})
collection.insert_many(docs)
print(f"Loaded {len(docs)} documents into {database}.playlists")
