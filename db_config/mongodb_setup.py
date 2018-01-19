import pymongo


class Data_base:
    def __init__(self, db_name):
        self.db_name = db_name

    def connect_db(self):
        client = pymongo.MongoClient('192.168.128.231:27017')
        db = client[self.db_name]
        return db

