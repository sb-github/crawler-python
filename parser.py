import pymongo
from datetime import datetime, timedelta
import re

array_vacancies = []
raw_vacancy = []


def data_b(collection_name):
    mongodb = 'mongodb://127.0.0.1:27017'
    client = pymongo.MongoClient(mongodb)
    db = client['Vacancies']
    posts = db[collection_name]
    return posts


def get_vacancy(status):
    array_vacancies.clear()
    data_vacancy = data_b('vacancy')
    # current
    date_time1 = datetime.today()
    # current - 20
    date_time2 = timedelta(minutes=20)
    for vacancy in data_vacancy.find({'status': status, 'created_date': {'$gte': date_time1 - date_time2, '$lt': date_time1}}):
        change_status('vacancy', vacancy)
        array_vacancies.append(vacancy)


def change_status(name_database, data_vacancy):
    data_base = data_b(name_database)
    if data_vacancy['status'] == 'NEW':
        data_base.update({'_id': data_vacancy['_id']}, {'$set': {'status': 'IN_PROCESS', 'modified_date': datetime.now()}})
    elif data_vacancy['status'] == 'IN_PROCESS':
        data_base.update({'_id': data_vacancy['_id']}, {'$set': {'status': 'PROCESSED', 'modified_date': datetime.now()}})
    else:
        data_base.update({'_id': data_vacancy['_id']}, {'$set': {'status': 'FAILED', 'modified_date': datetime.today()}})


def set_parsed_vacancy():
    try:
        raw_vacancy.clear()
        data_base = data_b('parsed_vacancy')
        get_vacancy('IN_PROCESS')
        for vacancy in array_vacancies:
            words = re.sub(u'[^А-Яа-яA-Za-z\s]*', u'', vacancy['raw'])
            skills = check_stop_words(words.split())
            print(skills)
            raw_vacancy.append(
                {
                    'vacancy_id': vacancy['_id'],
                    'crawler_id': vacancy['crawler_id'],
                    'link': vacancy['link'],
                    'raw_vacancy': skills,
                    'result': 'NEW'
                }
            )
            change_status('vacancy', vacancy)
        data_base.insert_many(raw_vacancy)
    except:
        print('The list is empty')


def check_stop_words(words):
    data_stop_word = data_b('stop_words')
    for word in words:
        for stop_word in data_stop_word.find({}):
            if word == stop_word['key']:
                words.remove(word)
    return words


# new, in the process, processed


get_vacancy('NEW')
set_parsed_vacancy()
