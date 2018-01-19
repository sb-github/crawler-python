import yaml, sys

import pymongo

from os.path import dirname



class Data_base:
    
    def __init__(self, db_name):
        self.db_name = db_name


    def get_mongo_ip(self):
        project_root = dirname(sys.path[0])
        yaml_rel_path = "/resources/config.yaml"
        yaml_path = project_root + yaml_rel_path
        with open(yaml_path) as yaml_file:
            conf = yaml.load(yaml_file)
        mongo_ip = conf['mongo_db']['ip']
        return mongo_ip


    def connect_db(self):
        client = pymongo.MongoClient(self.get_mongo_ip())
        db = client[self.db_name]
        return db
