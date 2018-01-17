import pymongo
from datetime import datetime, timedelta
import re

array_vacancies = []


class Data_base:
    def __init__(self, collection_name):
        self.address = 'mongodb://192.168.128.231:27017'
        self.collection_name = collection_name

    """The connection to the database"""
    def connect_db(self):
        client = pymongo.MongoClient(self.address)
        db = client['сrawler']
        posts = db[self.collection_name]
        return posts


class Parser_vacancy:
    def __init__(self):
        self.status = 'INATIVE'

    """this function get vacancies in interval 20 minutes(time create_vacancy + 20 minutes)"""
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
    """Change status and modified_date depending on the current"""
    def change_status(self, name_database, data_vacancy):
        data_base = Data_base(name_database).connect_db()
        if data_vacancy['status'] == 'NEW':
            data_base.update({'_id': data_vacancy['_id']},
                             {'$set': {'status': 'IN_PROCESS', 'modified_date': datetime.now()}})
            self.status = 'IN_PROCESS'
        elif data_vacancy['status'] == 'IN_PROCESS':
            data_base.update({'_id': data_vacancy['_id']},
                             {'$set': {'status': 'PROCESSED', 'modified_date': datetime.now()}})
            self.status = 'PROCESSED'
        else:
            data_base.update({'_id': data_vacancy['_id']},
                             {'$set': {'status': 'FAILED', 'modified_date': datetime.today()}})
            self.status = 'FAILED'

    """Cleaning raw from others symbols, signs, stop_words. And division the words"""
    def set_parsed_vacancy(self):
        try:
            parsed_vacancy.clear()
            data_base = Data_base('parsed_vacancy').connect_db()
            self.get_vacancy('IN_PROCESS')
            for vacancy in array_vacancies:
                s = 'Hello!@#!%!#&&!*!#$#%@*+_{ world!'
                reg = re.compile("[^а-яёїієґьщ'a-z0-9 ]+-")
                words = reg.sub('', vacancy['raw'])
                for junk_char in "%$@*.!&,:;•/\—)[]+(»«":
                    words = words.replace(junk_char, ' ')
                skills = self.check_stop_words(words.split())
                parsed_vacancy.append(
                    {
                        'vacancy_id': vacancy['_id'],
                        'crawler_id': vacancy['crawler_id'],
                        'link': vacancy['link'],
                        'raw_vacancy': list(set(skills)),
                        'result': 'NEW'
                    }
                )
                self.change_status('vacancy', vacancy)
            data_base.insert_many(parsed_vacancy)
        except:
            self.status = 'FAILED'

    def check_stop_words(self, words):
        data_stop_word = Data_base('stop_words').connect_db()
        for word in words:
            for stop_word in data_stop_word.find({}):
                if word == stop_word['key']:
                    words.remove(word)
        return words

    def run(self):
        try:
            self.get_vacancy('NEW')
        except:
            pass
        try:
            self.set_parsed_vacancy()
        except:
            pass
