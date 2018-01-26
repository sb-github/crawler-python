import pymongo
from datetime import datetime
import logging.config


class Data_base:
    def __init__(self, collection_name):
        self.collection_name = collection_name

    """The connection to the database"""
    def connect_db(self):
        client = pymongo.MongoClient('192.168.128.231:27017')
        db = client['crawler']
        posts = db[self.collection_name]
        return posts


class Graph_maker:
    def __init__(self):
        self.status = 'INATIVE'

    def delete_repeat_connect(self, con, sub_skill):
        for indx in range(len(con)):
            if con[indx]['subskill'] == sub_skill['subskill']:
                del con[indx]
                break

    def insert_one_skill(self, arr_raw_vacancy, con, parser_id, data_graph_skill, i, pars_vac):
        dict_skill = {'crawler_id': pars_vac['crawler_id'], 'skill': arr_raw_vacancy[i],
                      'created_date': datetime.today(), 'modified_date': datetime.now()}
        con.clear()

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

    def insert_one_subskill(self, parser_id1, pars_vac, arr_raw_vacancy, a, con):
        parser_id1.clear()
        parser_id1.append(pars_vac['_id'])
        sub_skill = {'subskill': arr_raw_vacancy[a], 'weight': 1, 'parser_id': parser_id1}
        con.append(sub_skill)

    def scan_sub_skill(self, arr_sub_skill, arr_raw_vacancy, a, parser_id, parser_id1, arr_pars_id, pars_vac,
                       arr_weight, con):

        if arr_sub_skill.count(arr_raw_vacancy[a]) >= 1:
            parser_id.clear()
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
            self.insert_one_subskill(parser_id1, pars_vac, arr_raw_vacancy, a, con)

    def graph_maker(self):
        
        logging.config.fileConfig('logging.conf')
        logger = logging.getLogger("pythonApp")

        logger.info("Graph_maker started. Let's go)")
        data_vacancy = Data_base('parsed_vacancy').connect_db()
        data_graph_skill = Data_base('graph_skill').connect_db()
        logger.info("Connection to the database...")
        arr_raw_vacancy = []
        parser_id = []
        parser_id1 = []
        arr_graph_skill = []
        arr_graph_connects = []
        con = []
        arr_sub_skill = []
        arr_pars_id = []
        arr_weight = []
        num = 0
        try:
            self.change_status('parsed_vacancy', "NEW")
            # select one vacancies from all
            for pars_vac in data_vacancy.find({'status': 'IN_PROCESS'}):
                num += 1
                logger.info("Vacancy number %s of %s in the process", num, data_vacancy.find({'status': 'IN_PROCESS'}).count())
                arr_raw_vacancy.clear()
                # select all skills
                for pars_skill in pars_vac['raw_vacancy']:
                    arr_raw_vacancy.append(pars_skill)
                # check vacancy on сrawler_id
                logger.info("Verification vacancy on 'сrawler_id'")
                data_graph_sk = data_graph_skill.find({'crawler_id': pars_vac['crawler_id']})
                if data_graph_sk.count() != 0:
                    arr_graph_skill.clear()
                    arr_graph_connects.clear()
                    # if crawler_id has, then we read data there is in graph_skill
                    logger.info("Read data skill from collection graph_skill")
                    for graph_skill in data_graph_sk:
                        arr_graph_skill.append(graph_skill['skill'])
                        arr_graph_connects.append(graph_skill['connects'])
                    # take one word from raw_vacancy
                    for i in range(len(arr_raw_vacancy)):
                        # check whether the word skill
                        logger.info("Verification word with skill")
                        if arr_graph_skill.count(arr_raw_vacancy[i]) == 1:
                            connects = arr_graph_connects[arr_graph_skill.index(arr_raw_vacancy[i])]
                            arr_sub_skill.clear()
                            arr_pars_id.clear()
                            arr_weight.clear()

                            for connect in connects:
                                arr_sub_skill.append(connect['subskill'])
                                arr_pars_id.append(connect['parser_id'])
                                arr_weight.append(connect['weight'])

                            con.clear()
                            logger.info("Added word to the sub_skill...")
                            for s in arr_graph_connects[arr_graph_skill.index(arr_raw_vacancy[i])]:
                                con.append(s)
                            """if the word is skill, then word goes through a cycle with all words, but word,
                            which is a skill """

                            # The scan starts sub_skills, if word == sub_skill, then start his updating
                            for a in range(i):
                                self.scan_sub_skill(arr_sub_skill, arr_raw_vacancy, a, parser_id, parser_id1,
                                                    arr_pars_id, pars_vac, arr_weight, con)

                            for b in range(i + 1, len(arr_raw_vacancy)):
                                self.scan_sub_skill(arr_sub_skill, arr_raw_vacancy, b, parser_id, parser_id1,
                                                    arr_pars_id, pars_vac, arr_weight, con)
                            data_graph_skill.update({'crawler_id': pars_vac['crawler_id'], 'skill': arr_raw_vacancy[i]},
                                                    {'$set': {'modified_date': datetime.now(), 'connects': con}})
                            logger.info("Update skill")
                        # if word is not skill, then creates a new skill with their sub_skills
                        else:
                            self.insert_one_skill(arr_raw_vacancy, con, parser_id, data_graph_skill, i, pars_vac)
                            logger.info("Adding skills to the document")
                # if no сrawler_id, then creates skills with their sub_skills
                else:
                    for i in range(len(arr_raw_vacancy)):
                        self.insert_one_skill(arr_raw_vacancy, con, parser_id, data_graph_skill, i, pars_vac)
                        logger.info("Adding skills to the document")
                logger.info("Vacancy number %s in the processed", num)

            self.change_status('parsed_vacancy', "IN_PROCESS")
        except:
            logger.error("FAILED! Stop the process at vacancy number %s in the processed", num)
            self.change_status('parsed_vacancy', 'FAILED')

    def change_status(self, name_database, status):
        data_base = Data_base(name_database).connect_db()
        if status == 'NEW':
            data_base.update_many({}, {'$set': {'status': 'IN_PROCESS', 'modified_date': datetime.now()}})
            self.status = 'IN_PROCESS'
        elif status == 'IN_PROCESS':
            data_base.update_many({}, {'$set': {'status': 'PROCESSED', 'modified_date': datetime.now()}})
            self.status = 'PROCESSED'
        else:
            data_base.update_many({}, {'$set': {'status': 'FAILED', 'modified_date': datetime.today()}})
            self.status = 'FAILED'

    def run(self):
        self.graph_maker()