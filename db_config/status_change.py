from mongodb_setup import Data_base

collection = Data_base("crawler").connect_db()
cursor = collection.find({'_id': id})
res = {}
for _ in cursor:
    res = _

res['status'] = 'STATUS'