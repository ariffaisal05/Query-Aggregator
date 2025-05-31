from pymongo import MongoClient

def connect_to_mongodb(db_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[db_name]
    collection = db[collection_name]
    return collection
