import logging.config
import re
from datetime import datetime
from functools import partial
from os import path
import sys
from multiprocessing.dummy import Pool

sys.path.append('..')

from db_config.mongodb_setup import Data_base


class Parser_vacancy:
    def __init__(self):
        self.data_base = Data_base('crawler').connect_db()
        self.log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
        logging.config.fileConfig(self.log_file_path)
        self.logger = logging.getLogger('parserApp')
        self.num = 0
        self.array_vacancies = []

    """ This function get vacancies in interval 20 minutes(time create_vacancy + 20 minutes) """
    def get_vacancy(self, status):
        self.array_vacancies.clear()
        data_vacancy = self.data_base['vacancy']
        self.logger.info("Connection to the database...")
        try:
            p = Pool()
            p.map(self.array_vacancies.append, data_vacancy.find({'status': status}))
            self.change_status('vacancy', status)
            self.logger.info("Data from documents 'vacancy' were take successfully")
        except:
            self.logger.error("FAILED! Error when connecting to database")
            raise SystemError('In parser detected error, look in parser.log')

    """ Change status and modified_date depending on the current """
    def change_status(self, name_database, status):
        data_base = self.data_base[name_database]
        if status == 'NEW':
            data_base.update_many({'status': 'NEW'}, {'$set': {'status': 'IN_PROCESS', 'modified_date': datetime.now()}})
        elif status == 'IN_PROCESS':
            data_base.update_many({'status': 'IN_PROCESS'}, {'$set': {'status': 'PROCESSED', 'modified_date': datetime.now()}})
        else:
            data_base.update_many({}, {'$set': {'status': 'FAILED', 'modified_date': datetime.today()}})

    def main_parser_part(self, parsed_vacancy, vacancy):
        self.num += 1
        reg = re.compile("[^а-яёїієґьъщ'a-z0-9 ]+-")
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
        self.logger.info("Vacancy number %s of %s in the processed", self.num, len(self.array_vacancies))

    """ Cleaning raw from others symbols, signs, stop_words. And division the words """
    def set_parsed_vacancy(self):
        try:
            parsed_vacancy = []
            data_base = self.data_base['parsed_vacancy']
            self.get_vacancy('IN_PROCESS')
            p = Pool()
            func = partial(self.main_parser_part, parsed_vacancy)
            p.map(func, self.array_vacancies)
            self.change_status('vacancy', 'IN_PROCESS')
            data_base.insert_many(parsed_vacancy)
            self.logger.info("Data from the document 'vacancy' was processed successfully,"
                             " and were listed in table parsed_vacancy")
        except:
            self.change_status('vacancy', 'FAILED')
            self.logger.error("FAILED! Error when data processing")
            raise SystemError("In parser detected error, look in parser.log")

    def check_stop_words(self, words):
        data_stop_word = self.data_base['stop_words']
        for word in words:
            for stop_word in data_stop_word.find({}):
                if word == stop_word['key']:
                    words.remove(word)
        return words

    def run(self):
        """ To collect all vacancies with status NEW """
        self.logger.info("Graph_maker started. Let's go)")
        self.get_vacancy('NEW')

        """ Main function for document parsed_vacancy """
        self.set_parsed_vacancy()
