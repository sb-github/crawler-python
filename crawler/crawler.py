
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
import requests, math, logging, inspect, sys, time

from lxml import html
from bson import ObjectId
from multiprocessing.dummy import Pool
from datetime import datetime as dt

sys.path.append('..')

from db_config.mongodb_setup import Data_base


NEW = "NEW"
INACTIVE= "INACTIVE"
SETUP = "SETUP"
BANNED = "BANNED"
IN_PROCESS = "IN_PROCESS"
PROCESSED = "PROCESSED"
FAILED = "FAILED"


def fib(n):
    """
    returns n's member of fibonacci sequence
    """
    if n == 1 or n == 2:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


def get_bin(url, headers, logger, attempts=10):
    '''
    gets response binary for url via request
    '''
    req = requests.get(url, headers=headers, timeout=5)
    logger.info("request status code - %s for %s", req.status_code, url)
    attempts_qty = 1
    while req.status_code != 200 and attempts_qty > attempts:
        time.sleep(fib(attempts_qty))
        req = requests.get(url, headers=headers, timeout=5)
        logger.info("RETRY request status code - %s for %s", req.status_code, url)
        attempts_qty += 1
    if req.status_code == 200:
        logger.info("request status code - %s for %s", req.status_code, url)
        binary = req.content
        return binary
    else:
        logger.info("request FAILED - %s for %s", req.status_code, url)


def extract_1_num(string):
    '''
    extracts number (int) from string with one number in it
    '''
    lis = string.rsplit()
    num_list = list(filter(lambda x: x.isdigit(), lis))
    num = num_list[0]
    num_int = int(num)
    return num_int


def get_title_n_raw_from_bin(ws, bin_html):
    '''
    takes websource dictionary (configuration) and returns title and raw
    '''
    tree = html.fromstring(bin_html)
    title_elem = tree.xpath(ws['title_xpath'])[0]
    title = title_elem.text_content()
    raw_elem = tree.xpath(ws['raw_xpath'])[0]
    raw = ""
    for text_piece in raw_elem.itertext():
        raw += " "
        raw += text_piece
    return title, raw


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
    status = INACTIVE         # current status of crawler instance ("INACTIVE", "IN_PROCESS", "FAILED", "PROCESSED")
    skill = None              # search skill criteria
    websources = {}           # key - name of websource, value - dict with configurations
    page_links_dict = {}      # key - name of websource, value - list of page (pagination pages) links with vacancies
    vac_links_dict = {}       # key - name of websourse, value - list of vacanciy's links
    f_vac_links_dict = {}     # filtered vac_links_dict
    vacancies_dict = {}       # dict of vacancies dicts with link, title, raw


<<<<<<< HEAD:crawler/crawler.py
    def __init__(self, _id, env="test"):
=======
    def __init__(self, _id, env='test'):
>>>>>>> master:crawler/crawler.py
        '''
        websources - websources objects dictionary (key - ws name, value - config)
        skill - search criteria (skill)
        '''
        self._id = _id
        self.env = env
        self.logger = self.init_logger()
        self.db = Data_base('crawler')


    def init_logger(self):
        '''
        Initialize log file.
        '''
<<<<<<< HEAD:crawler/crawler.py
        logger = logging.getLogger('crawler_app %s' % self._id)
=======
        logger = logging.getLogger('crawler_app {}'.format(self._id))
>>>>>>> master:crawler/crawler.py
        logger.setLevel(logging.INFO)

        # create a file handler
        handler = logging.FileHandler('crawler %s %s.log' % (self._id, dt.now()))
        handler.setLevel(logging.INFO)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(handler)
        return logger



    def get_crawler_dict_from_db(self):
        '''
        get crawler instance dictionary from database (contains search skill)
        '''
        cursor = self.db.connect_db().crawler.find({'_id':ObjectId(self._id)})
        for crawler in cursor:
            crawler_dict = crawler
        return crawler_dict


    def read_skill_from_db(self):
        '''
        get search skill from db
        '''
        fname = inspect.stack()[0][3]
        self.logger.info('%s - %s', SETUP, fname)
        skill = self.get_crawler_dict_from_db()['search_condition']
        self.skill = skill


    def read_websourses_from_db(self):
        '''
        get websources configuration from db
        '''
        fname = inspect.stack()[0][3]
        self.logger.info('%s - %s', SETUP, fname)
        cursor = self.db.connect_db().websource.find()
        for ws in cursor:
            self.websources.update({ws.pop('name'):ws})


    def collect_pages_links(self):
        '''
        Collects page links (pagination pages)
        '''
        fname = inspect.stack()[0][3]
        self.logger.info('%s - %s', IN_PROCESS, fname)
        for ws_name, ws in self.websources.items():
            search_pattern = ws['base_url'] + ws['search_pattern']
            pag_start = ws['pagination_start']
            headers = ws['headers']
            pars_skill = urllib.parse.quote_plus(self.skill)
            url = search_pattern.format(skill=pars_skill, page=pag_start)
            tree = html.fromstring(get_bin(url, headers, self.logger))
            jobs_qty_elem = tree.xpath(self.websources[ws_name]['jobs_qty_xpath'])[0]
            jobs_qty_text = jobs_qty_elem.text_content()
            jobs_qty = extract_1_num(jobs_qty_text)
            pages_qty = math.ceil(jobs_qty/ws['pagination'])
            pages_range = range(pag_start, pages_qty + pag_start)
            ws_page_list = [search_pattern.format(skill=pars_skill, page=x) for x in pages_range]
            self.page_links_dict[ws_name] = ws_page_list
            self.logger.info("%s - %s - page links collected for %s", IN_PROCESS, fname, ws_name)


    def get_link(self, item):
        '''
        takes ws name and page url, then returns vacancies links on this page
        '''
        ws_name, page_link = item
        ws = self.websources[ws_name]
        link_xpath = ws['link_xpath']
        link_is_abs = ws['absolute_links']
        base_url = ws['base_url']
        headers = ws['headers']
        page_bin_html = get_bin(page_link, headers, self.logger)
        vac_links = get_vac_links(base_url, page_bin_html, link_xpath, link_is_abs)
        return vac_links


    def prepare_page_links(self, item):
        '''
        prepares page links and creates pool in whitch we use function get_link that collects vac links
        '''
        fname = inspect.stack()[0][3]
        self.logger.info('%s - %s', IN_PROCESS, fname)
        ws_name, page_links = item
        wsname_and_link_seq = list(map(lambda x: (ws_name, x), page_links))
        pool = Pool(10)
        ws_vac_links = []
        ws_vac_links_2d = pool.map(self.get_link, wsname_and_link_seq)
        for lis in ws_vac_links_2d:
            ws_vac_links.extend(lis)
        pool.close()
        pool.join()
        self.vac_links_dict[ws_name] = ws_vac_links
        self.logger.info("%s - %s - vacancies links collected for %s", IN_PROCESS, fname, ws_name)


    def filter_vac_links(self):
        '''
        Cleans up lists in vac_links_dict ( deletes repeating values )
        '''
        fname = inspect.stack()[0][3]
        self.logger.info('%s - %s', IN_PROCESS, fname)
        filtered_links = dict()
        for ws_name, links in self.vac_links_dict.items():
            link_set = set(links)
            link_list = list(link_set)
            filtered_links[ws_name] = link_list
<<<<<<< HEAD:crawler/crawler.py
            self.logger.info("%s - %s - vacancies links filtered for %s", IN_PROCESS, fname, ws_name)
        self.f_vac_links_dict = filtered_links
=======
            message = "{} - {} - vacancies links filtered for {}".format(IN_PROCESS, fname, ws_name)
            # self.logger.info(message)
        self.vac_links_dict = filtered_links

>>>>>>> master:crawler/crawler.py


    def collect_vacancy(self, vac_tuple):
        """
        collects vacancy's raw from websource. Takes vac tuple with websourse name and link,
        and writes it to vacancies_dict
        """
        ws_name, vac_link = vac_tuple
        fname = inspect.stack()[0][3]
        ws = self.websources[ws_name]
        headers = ws['headers']
        bin_html = get_bin(vac_link, headers, self.logger)
        if bin_html != None:
            title, raw = get_title_n_raw_from_bin(ws, bin_html)
            self.vacancies_dict[ws_name].append({
                'crawler_id':ObjectId(self._id),
                'link':vac_link,
                'title':title.lower(),
                'raw':raw.lower(),
                'status':NEW,
                'created_date': dt.now(),
                'modified_date': dt.now(),
            })
            self.logger.info("%s - %s - vacancy '%s' (title, raw) collected from %s", IN_PROCESS, fname, title, ws_name)


    def process_pool_for_collecting_links(self, item):
        """
        procces pool for collecting links
        """
        fname = inspect.stack()[0][3]
        self.logger.info('%s - %s', IN_PROCESS, fname)
        ws_name, vac_links = item
        self.vacancies_dict[ws_name] = []
        vac_links = list(map(lambda x: (ws_name, x), vac_links))
        pool = Pool(10)
        pool.map(self.collect_vacancy, vac_links)
        pool.close()
        pool.join()


    def process_ws_pool(self, func, sequence):
        """
        takes function and sequence. Than creates pool with separetaed thread for each websource
        """
        fname = inspect.stack()[0][3]
        self.logger.info('%s - %s', IN_PROCESS, fname)
        ws_qty = len(self.websources)
        pool = Pool(ws_qty)
        pool.map(func, sequence)
        pool.close()
        pool.join()


    def write_vacancies_in_db(self):
        '''
        write vacancies to db, packes by websources
        '''
        fname = inspect.stack()[0][3]
        self.logger.info('%s - %s', IN_PROCESS, fname)
        for ws_name, vacancies in self.vacancies_dict.items():
            if vacancies:
                self.db.connect_db().vacancy.insert_many(vacancies)
                self.vacancies_dict[ws_name].clear()
                self.logger.info("%s - %s - vacancies for has been written to database from %s", IN_PROCESS, fname, ws_name)


    def setup(self):
        '''
        setup method, use it every time after you've initialyzed nes Crawler instance
        '''
        try:
            self.logger.info('%s START', SETUP)
            self.status = SETUP

            self.read_skill_from_db()
            self.read_websourses_from_db()

            self.status = INACTIVE
            self.logger.info('%s FINISH', SETUP)

        except:
            self.status = FAILED
            self.logger.exception("crawler setup error")
            raise SystemError('crawler setup error, look crawler log for information')


    def run(self):
        '''
        after setup you can run this method to collect and write vacancies to db
        '''
        try:
            self.logger.info("%s START", IN_PROCESS)
            self.status = IN_PROCESS

            self.collect_pages_links()
            self.process_ws_pool(self.prepare_page_links, self.page_links_dict.items())
            self.filter_vac_links()
            self.process_ws_pool(self.process_pool_for_collecting_links, self.f_vac_links_dict.items())
            self.write_vacancies_in_db()

            self.status = PROCESSED
            self.logger.info("%s FINISH", IN_PROCESS)

        except:
            self.status = FAILED
            self.logger.exception("crawler runtime error")
            raise SystemError('crawler runtime error, look crawler log for information')
