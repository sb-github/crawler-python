from datetime import datetime
import logging.config
from math import ceil
from multiprocessing.dummy import Process, Pool
import sys
from os import path

sys.path.append('..')

from db_config.mongodb_setup import Data_base


class Graph_maker:

    def __init__(self):
        self.arr_graph_skill = []
        self.arr_graph_connects = []
        self.data_base = Data_base('crawler').connect_db()
        self.log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
        logging.config.fileConfig(self.log_file_path)
        self.logger = logging.getLogger('pythonApp')
        self.data_vacancy = self.data_base['parsed_vacancy']
        self.data_graph_skill = self.data_base['graph_skill']
        self.count_vacancy = self.data_vacancy.find({'status': 'NEW'}).count()
        self.current_num = 0
        self.arr_sub_skill = []
        self.arr_pars_id = []
        self.arr_weight = []

    def delete_repeat_connect(self, con, sub_skill):
        for indx in range(len(con)):
            if con[indx]['subskill'] == sub_skill['subskill']:
                del con[indx]
                break

    def insert_one_skill(self, arr_raw_vacancy, con, data_graph_skill, i, pars_vac):
        dict_skill = {'crawler_id': pars_vac['crawler_id'], 'skill': arr_raw_vacancy[i],
                      'created_date': datetime.today(), 'modified_date': datetime.now()}
        con.clear()
        parser_id = []
        for a in range(i):
            parser_id.clear()
            parser_id.append(pars_vac['_id'])
            sub_skill = {'subskill': arr_raw_vacancy[a], 'weight': 1, 'parser_id': parser_id}
            con.append(sub_skill)
        dict_skill.update({'connects': con})

        for b in range(i + 1, len(arr_raw_vacancy)):
            parser_id.clear()
            parser_id.append(pars_vac['_id'])
            sub_skill = {'subskill': arr_raw_vacancy[b], 'weight': 1, 'parser_id': parser_id}
            con.append(sub_skill)

        dict_skill.update({'connects': con})
        data_graph_skill.insert(dict_skill)

    def insert_one_subskill(self, pars_vac, arr_raw_vacancy, a, con):
        parser_id = [pars_vac['_id']]
        sub_skill = {'subskill': arr_raw_vacancy[a], 'weight': 1, 'parser_id': parser_id}
        con.append(sub_skill)

    def scan_sub_skill(self, arr_sub_skill, arr_raw_vacancy, a, arr_pars_id, pars_vac,
                       arr_weight, con):
        parser_id = []
        if arr_sub_skill.count(arr_raw_vacancy[a]) == 1:
            sub_skill = {'subskill': arr_sub_skill[arr_sub_skill.index(arr_raw_vacancy[a])]}
            pars_id = arr_pars_id[arr_sub_skill.index(arr_raw_vacancy[a])]
            for p_id in pars_id:
                parser_id.append(p_id)
            parser_id.append(pars_vac['_id'])
            sub_skill.update({'weight': arr_weight[arr_sub_skill.index(arr_raw_vacancy[a])] + 1,
                              'parser_id': parser_id})

            self.delete_repeat_connect(con, sub_skill)
            con.append(sub_skill)
        # if word != sub_skill, then word added as a skill
        else:
            self.insert_one_subskill(pars_vac, arr_raw_vacancy, a, con)

    def get_array_data_graph(self, graph_skill):
        self.arr_graph_skill.append(graph_skill['skill'])
        self.arr_graph_connects.append(graph_skill['connects'])

    def get_graph_maker(self):
        self.logger.info("Graph_maker started. Let's go)")

        self.logger.info("Connection to the database...")

        arr_raw_vacancy = []
        arr_vacancies = []
        con = []
        arr_sub_skill = []
        arr_pars_id = []
        arr_weight = []

        try:
            try:
                for pars_vac in self.data_vacancy.find({'status': 'NEW'}).limit(25):
                    arr_vacancies.append(pars_vac)
                self.change_status(arr_vacancies, self.data_vacancy)
            except:
                self.logger.error("FAILED! Error when connecting to database")
                raise SystemError('In graph_maker detected error, look in graph_maker.log')
            # select one vacancies from all
            for index in range(len(arr_vacancies)):
                self.current_num += 1
                self.logger.info("Vacancy number %s of %s in the process", self.current_num, self.count_vacancy)
                arr_raw_vacancy.clear()
                # select all skills
                for pars_skill in arr_vacancies[index]['raw_vacancy']:
                    arr_raw_vacancy.append(pars_skill)
                # check vacancy on сrawler_id
                self.logger.info("Verification vacancy on 'сrawler_id'")
                data_graph_sk = self.data_graph_skill.find({'crawler_id': arr_vacancies[index]['crawler_id']})
                if data_graph_sk.count() != 0:
                    self.arr_graph_skill.clear()
                    self.arr_graph_connects.clear()
                    # if crawler_id has, then we read data there is in graph_skill
                    self.logger.info("Read data skill from collection graph_skill")
                    p = Pool(20)
                    p.map(self.get_array_data_graph, data_graph_sk)
                    # take one word from raw_vacancy
                    for i in range(len(arr_raw_vacancy)):
                        # check whether the word skill
                        self.logger.info("Verification word with skill")
                        if self.arr_graph_skill.count(arr_raw_vacancy[i]) == 1:
                            connects = self.arr_graph_connects[self.arr_graph_skill.index(arr_raw_vacancy[i])]
                            arr_sub_skill.clear()
                            arr_pars_id.clear()
                            arr_weight.clear()

                            for connect in connects:
                                arr_sub_skill.append(connect['subskill'])
                                arr_pars_id.append(connect['parser_id'])
                                arr_weight.append(connect['weight'])

                            con.clear()
                            self.logger.info("Added word to the sub_skill...")
                            for s in self.arr_graph_connects[self.arr_graph_skill.index(arr_raw_vacancy[i])]:
                                con.append(s)
                            """if the word is skill, then word goes through a cycle with all words, but word,
                            which is a skill """

                            # The scan starts sub_skills, if word == sub_skill, then start his updating
                            for a in range(i):
                                self.scan_sub_skill(arr_sub_skill, arr_raw_vacancy, a,
                                                    arr_pars_id, arr_vacancies[index], arr_weight, con)

                            for b in range(i + 1, len(arr_raw_vacancy)):
                                self.scan_sub_skill(arr_sub_skill, arr_raw_vacancy, b,
                                                    arr_pars_id, arr_vacancies[index], arr_weight, con)
                            self.data_graph_skill.update({'crawler_id': arr_vacancies[index]['crawler_id'],
                                                          'skill': arr_raw_vacancy[i]}, {
                                '$set': {'modified_date': datetime.now(), 'connects': con}})
                            self.logger.info("Update skill")
                        # if word is not skill, then creates a new skill with their sub_skills
                        else:
                            self.insert_one_skill(arr_raw_vacancy, con, self.data_graph_skill, i,
                                                  arr_vacancies[index])
                            self.logger.info("Adding skills to the document")
                # if no сrawler_id, then creates skills with their sub_skills
                else:
                    for i in range(len(arr_raw_vacancy)):
                        self.insert_one_skill(arr_raw_vacancy, con, self.data_graph_skill, i, arr_vacancies[index])
                        self.logger.info("Adding skills to the document")
                self.logger.info("Vacancy number %s in the processed", self.current_num)

            self.change_status(arr_vacancies, self.data_vacancy)
        except:
            self.change_status(arr_vacancies, self.data_vacancy)
            self.logger.exception("FAILED! Stop the process at vacancy number %s", self.current_num)
            raise SystemError('In graph_maker detected error, look in graph_maker.log')

    def change_status(self, arr_vacancies, data_base):
        for vacancy in arr_vacancies:
            if vacancy['status'] == 'NEW':
                vacancy['status'] = 'IN_PROCESS'
                data_base.update({'_id': vacancy['_id']}, {'$set': {'status': 'IN_PROCESS',
                                                                    'modified_date': datetime.now()}})
            elif vacancy['status'] == 'IN_PROCESS':
                vacancy['status'] = 'PROCESSED'
                data_base.update({'_id': vacancy['_id']}, {'$set': {'status': 'PROCESSED',
                                                                    'modified_date': datetime.now()}})
            else:
                data_base.update({'_id': vacancy['_id']}, {'$set': {'status': 'FAILED',
                                                                    'modified_date': datetime.today()}})

    def run(self):
        jobs = []
        for i in range(int(ceil(self.count_vacancy/25))):
            p = Process(target=self.get_graph_maker())
            jobs.append(p)
            p.start()
            p.join()
