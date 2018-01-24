import sys
import pymongo

sys.path.append('..')

from resources import config


class Data_base:
    
    def __init__(self, db_name):
        self.db_name = db_name

    def connect_db(self):
        client = pymongo.MongoClient(config.IP)
        db = client[self.db_name]
        return db
