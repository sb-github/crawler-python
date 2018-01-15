import pymongo
from datetime import datetime, timedelta
import re

array_vacancies = []
raw_vacancy = []


class Data_base:
    def __init__(self, collection_name):
        self.address = 'mongodb://127.0.0.1:27017'
        self.collection_name = collection_name

    def connect_db(self):
        client = pymongo.MongoClient(self.address)
        db = client['Vacancies']
        posts = db[self.collection_name]
        return posts


class Parser_vacancy:
    def get_vacancy(self, status):
        array_vacancies.clear()
        data_vacancy = Data_base('vacancy').connect_db()
        # current
        date_time1 = datetime.today()
        # current - 20
        date_time2 = timedelta(minutes=20)
        for vacancy in data_vacancy.find(
                {'status': status, 'created_date': {'$gte': date_time1 - date_time2, '$lt': date_time1}}):
            self.change_status('vacancy', vacancy)
            array_vacancies.append(vacancy)

    # new, in the process, processed
    def change_status(self, name_database, data_vacancy):
        data_base = Data_base(name_database).connect_db()
        if data_vacancy['status'] == 'NEW':
            data_base.update({'_id': data_vacancy['_id']},
                             {'$set': {'status': 'IN_PROCESS', 'modified_date': datetime.now()}})
        elif data_vacancy['status'] == 'IN_PROCESS':
            data_base.update({'_id': data_vacancy['_id']},
                             {'$set': {'status': 'PROCESSED', 'modified_date': datetime.now()}})
        else:
            data_base.update({'_id': data_vacancy['_id']},
                             {'$set': {'status': 'FAILED', 'modified_date': datetime.today()}})

    def set_parsed_vacancy(self):
        try:
            raw_vacancy.clear()
            data_base = Data_base('parsed_vacancy').connect_db()
            self.get_vacancy('IN_PROCESS')
            for vacancy in array_vacancies:
                words = re.sub(u'[^А-Яа-яA-Za-z\s]*', u'', vacancy['raw'])
                skills = self.check_stop_words(words.split())
                raw_vacancy.append(
                    {
                        'vacancy_id': vacancy['_id'],
                        'crawler_id': vacancy['crawler_id'],
                        'link': vacancy['link'],
                        'raw_vacancy': skills,
                        'result': 'NEW'
                    }
                )
                self.change_status('vacancy', vacancy)
            data_base.insert_many(raw_vacancy)
        except:
            print('The list is empty')

    def check_stop_words(self, words):
        data_stop_word = Data_base('stop_words').connect_db()
        for word in words:
            for stop_word in data_stop_word.find({}):
                if word == stop_word['key']:
                    words.remove(word)
        return words


parser = Parser_vacancy()
parser.get_vacancy('NEW')
parser.set_parsed_vacancy()
