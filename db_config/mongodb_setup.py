import pymongo


class Data_base:
    def __init__(self, collection_name):
        self.collection_name = collection_name

    def connect_db(self):
        client = pymongo.MongoClient('192.168.128.231:27017')
        db = client['crawler']
        posts = db[self.collection_name]
        return posts
