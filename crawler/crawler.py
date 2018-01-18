''' 
Welcome to crawler module written on Python 3.6, it contains Crawler class and additional functions-helpers 
that you must NOT modify. 

MAKE SURE that you have pymongo and lxml installed.

To start crawling data:

1. Initialyze Crawler class instance and pass _id attribute (e.g. crawler_instanse = Crawler(_id="5a58bb84189bf2ae9b229efc")). 
2. Run crawler_instanse.setup() method to setup crawler.
3. Run crawler_instanse.run() method to start grabbing data and after this writing it to db.

'''

import urllib.parse
import requests, math
import pymongo
from lxml import html
from bson import ObjectId
from datetime import datetime as dt


def crawler_db():
    '''
    func that returns crawler database instance
    '''
    client = pymongo.MongoClient('192.168.128.231:27017')
    db = client['crawler']
    return db
    

def get_bin(url, headers):
    '''
    gets response binary for url via request
    '''
    req = requests.get(url, headers=headers, timeout=5)
    print("request status code - {}".format(req.status_code))
    binary = req.content
    return binary


def extract_1_num(string):
    '''
    extracts number (int) from string with one number in it
    '''
    lis = string.rsplit()
    num_list = list(filter(lambda x: x.isdigit(), lis))
    num = num_list[0]
    num_int = int(num)
    return num_int


def get_vac_links(base_url, bin_html, link_xpath, link_is_abs):
    '''
    gets vacancies links from html binary using given xpath, also transforms links into
    absolute (base_url + rel_link) if links_is_abs is False
    '''
    tree = html.fromstring(bin_html)
    link_elems = tree.xpath(link_xpath)
    links_list = list(map(lambda x: dict(x.items())['href'], link_elems))
    if not link_is_abs:
        links_list = list(map(lambda x: base_url + x, links_list))
    return links_list


class Crawler(object):
    '''
    crawler class, has methods that scrap web services.
    give an _id from crawler collection as an argument
    '''
    log = ''                # crawler`s log
    status = 'INACTIVE'     # current status of crawler instance ("INACTIVE", "IN_PROCESS", "FAILED", "PROCESSED")
    skill = None            # search skill criteria
    websources = {}         # key - name of websource, value - dict with configurations
    page_links_dict = {}    # key - name of websource, value - list of page (pagination pages) links with vacancies
    vac_links_dict = {}     # key - name of websourse, value - list of vacanciy's links
    vacancies_dict = {}     # dict of vacancies dicts with link, title, raw


    def __init__(self, _id, env='test'):
        '''
        websources - websources objects dictionary (key - ws name, value - config)
        skill - search criteria (skill)
        '''
        self._id = _id
        self.env = env


    def write_print_log(self, string):
        '''
        print current log notation and add it to crawler_instance.log
        '''
        print(string)
        self.log += (string + "\n")


    def get_crawler_dict_from_db(self):
        '''
        get crawler instance dictionary from database (contains search skill)
        '''
        cursor = crawler_db().crawler.find({'_id':ObjectId(self._id)})
        for crawler in cursor:
            crawler_dict = crawler
        return crawler_dict


    def read_skill_from_db(self):
        '''
        get search skill from db
        '''
        skill = self.get_crawler_dict_from_db()['searchCondition']
        self.skill = skill


    def read_websourses_from_db(self):
        '''
        get websources configuration from db
        '''
        cursor = crawler_db().websource.find()
        for ws in cursor:
            self.websources.update({ws.pop('name'):ws})


    def collect_pages_links(self):
        '''
        Collects page links (pagination pages) 
        '''
        for ws_name, ws in self.websources.items():
            search_pattern = ws['base_url'] + ws['search_pattern']
            pag_start = ws['pagination_start']
            headers = ws['headers']
            pars_skill = urllib.parse.quote_plus(self.skill)
            tree = html.fromstring(get_bin(search_pattern.format(skill=pars_skill, page=pag_start), headers))
            jobs_qty_elem = tree.xpath(self.websources[ws_name]['jobs_qty_xpath'])[0]
            jobs_qty_text = jobs_qty_elem.text_content()
            jobs_qty = extract_1_num(jobs_qty_text)
            pages_qty = math.ceil(jobs_qty/ws['pagination'])
            pages_range = range(pag_start, pages_qty + pag_start)
            ws_page_list = [search_pattern.format(skill=pars_skill, page=x) for x in pages_range]
            self.page_links_dict[ws_name] = ws_page_list
            log = "page links collected for {}".format(ws_name)
            self.write_print_log(log)


    def collect_vac_links(self):
        '''
        Collects vacancies links from page links
        '''
        for ws_name, page_links in self.page_links_dict.items():
            ws = self.websources[ws_name]
            link_xpath = ws['link_xpath']
            link_is_abs = ws['absolute_links']
            base_url = ws['base_url']
            headers = ws['headers']
            ws_vac_links = []
            for page_link in page_links:
                page_bin_html = get_bin(page_link, headers)
                vac_links = get_vac_links(base_url, page_bin_html, link_xpath, link_is_abs)
                ws_vac_links.extend(vac_links)
            self.vac_links_dict[ws_name] = ws_vac_links
            log = "vacancies links collected for {}".format(ws_name)
            self.write_print_log(log)


    def filter_vac_links(self):
        '''
        Cleans up lists in vac_links_dict ( deletes repeating values )
        '''
        filtered_links = dict()

        for ws_name, links in self.vac_links_dict.items():
            link_set = set(links)
            link_list = list(link_set)
            filtered_links[ws_name] = link_list
            log = "vacancies links filtered for {}".format(ws_name)
            self.write_print_log(log)
        self.vac_links_dict = filtered_links


    def collect_vac_raws(self):
        '''
        Collects vanancies (title, raw) into vacancies list
        '''
        for ws_name, vac_links in self.vac_links_dict.items():
            ws = self.websources[ws_name]
            headers = ws['headers']
            ws_vacancies = []
            for vac_link in vac_links:
                bin_html = get_bin(vac_link, headers)
                tree = html.fromstring(bin_html)
                title_elem = tree.xpath(ws['title_xpath'])[0]
                title = title_elem.text_content()
                raw_elem = tree.xpath(ws['raw_xpath'])[0]
                raw = raw_elem.text_content()
                ws_vacancies.append({
                    'crawler_id':self._id,
                    'link':vac_link,
                    'title':title.lower(),
                    'raw':raw.lower(),
                    'status':'NEW',
                    'created_date': dt.now(),
                    'modified_date': dt.now(),
                })
                log = "vacancy '{}' (title, raw) collected from {}".format(title, ws_name)
                self.write_print_log(log)
            self.vacancies_dict[ws_name] = ws_vacancies


    def write_vacancies_in_db(self):
        '''
        write vacancies to db, packes by websources
        '''
        for ws_name, vacancies in self.vacancies_dict.items():
            crawler_db().vacancy.insert_many(vacancies)
            log = "vacancies for has been written to database from {}".format(ws_name)
            self.write_print_log(log)


    def setup(self):
        '''
        setup method, use it every time after you've initialyzed nes Crawler instance 
        '''
        self.status = "SETUP"

        self.read_skill_from_db()

        self.read_websourses_from_db()


    def run(self):
        '''
        after setup you can run this method to collect and write vacancies to db 
        '''
        self.status = "IN_PROCESS"

        self.collect_pages_links()

        self.collect_vac_links()

        self.filter_vac_links()

        self.collect_vac_raws()

        self.write_vacancies_in_db()

        self.status = "PROCESSED"
