import pymongo
from datetime import datetime, timedelta
import re
import logging.config

array_vacancies = []
parsed_vacancy = []


class Data_base:
    def __init__(self, collection_name):
        self.collection_name = collection_name

    """The connection to the database"""
    def connect_db(self):
        client = pymongo.MongoClient('192.168.128.231:27017')
        db = client['crawler']
        posts = db[self.collection_name]
        return posts


class Parser_vacancy:
    def __init__(self):
        logging.config.fileConfig('crawler_app/parser/logging.conf')
        self.logger = logging.getLogger('parserApp')

    """this function get vacancies in interval 20 minutes(time create_vacancy + 20 minutes)"""
    def get_vacancy(self, status):
        array_vacancies.clear()
        data_vacancy = Data_base('vacancy').connect_db()
        self.logger.info("Connection to the database...")
        # current
        # date_time1 = datetime.today()
        # current - 20
        # date_time2 = timedelta(minutes=20)
        # 'created_date': {'$gte': date_time1 - date_time2, '$lt': date_time1}
        try:
            for vacancy in data_vacancy.find({'status': status}):
                array_vacancies.append(vacancy)
            self.change_status('vacancy', status)
            self.logger.info("Data from documents 'vacancy' were take successfully")
        except:
            self.logger.error("FAILED! Error when connecting to database")
            raise SystemError('In parser detected error, look in parser.log')

    """Change status and modified_date depending on the current"""
    def change_status(self, name_database, status):
        data_base = Data_base(name_database).connect_db()
        if status == 'NEW':
            data_base.update_many({}, {'$set': {'status': 'IN_PROCESS', 'modified_date': datetime.now()}})
        elif status == 'IN_PROCESS':
            data_base.update_many({}, {'$set': {'status': 'PROCESSED', 'modified_date': datetime.now()}})
        else:
            data_base.update_many({}, {'$set': {'status': 'FAILED', 'modified_date': datetime.today()}})

    """Cleaning raw from others symbols, signs, stop_words. And division the words"""
    def set_parsed_vacancy(self):
        try:
            parsed_vacancy.clear()
            data_base = Data_base('parsed_vacancy').connect_db()
            self.get_vacancy('IN_PROCESS')
            num = 0
            for vacancy in array_vacancies:
                num += 1
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
                        'status': 'NEW',
                        'created_date': datetime.today(),
                        'modified_date': datetime.today()
                    })
                self.logger.info("Vacancy number %s of %s in the processed", num, len(array_vacancies))
            self.change_status('vacancy', 'IN_PROCESS')
            data_base.insert_many(parsed_vacancy)
            self.logger.info("""Data from the document 'vacancy' was processed successfully,
             and were listed in table parsed_vacancy""")
        except:
            self.change_status('vacancy', 'FAILED')
            self.logger.error("FAILED! Error when data processing")
            raise SystemError("In parser detected error, look in parser.log")

    def check_stop_words(self, words):
        data_stop_word = Data_base('stop_words').connect_db()
        for word in words:
            for stop_word in data_stop_word.find({}):
                if word == stop_word['key']:
                    words.remove(word)
        return words

    def run(self):
        """To collect all vacancies with status NEW"""
        self.logger.info("Graph_maker started. Let's go)")
        self.get_vacancy('NEW')

        """Main function for document parsed_vacancy"""
        self.set_parsed_vacancy()
