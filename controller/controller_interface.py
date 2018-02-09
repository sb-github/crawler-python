#!/usr/bin/python3

import time
import random
from threading import Thread
import logging
import sys
from uuid import uuid4
from bson import ObjectId

from .sub_procs import procs, create_subprocess, kill_process

sys.path.append('..')

from db_config.mongodb_setup import Data_base



# object constants
CRAWLER = 'CRAWLER'
PARSER = 'PARSER'

# status constants
STARTED = 'NEW'
FINISHED = 'PROCESSED'
TERMINATED = 'STOPPED'
FAILED = 'FAILED'
IN_PROCESS = 'IN_PROCESS'



class Controller:

    '''
    Controller class that gives you an opportunity to 
    manipulate such objects as Crawler and Parser.
    1. Use start_crawler() to run Crawler.
    2. Use start_parser() to run Parser.
    3. To terminate Crawler or Parser with force, use terminate_process().
    All details about objects creation will be written to log file 
    (in the directory where Controller is initialized). 
    '''

    def __init__(self, environ=None):
        self.environ = environ              # set environment
        self.objects = {}                   # key - uuid, value - dict with object data (type, status, internal_id)
        self.logger = self.init_logger()    # initialize log file
        self.db = self.db_setup()           # setup mongodb collection


    def set_up(self):
        pass


    def start(self):
        pass


    def status(self): 
        '''
        Get all running processes.
        '''
        #return self.objects
        res = []
        for uuid in self.objects:
            res.append(self.json_response(uuid))
        return res


    def shut_down(self):
        pass


    def start_crawler(self, uuid):
        '''
        Use this method in REST_API to create and launch Crawler.
        '''
        uuid = str(uuid)
        file = '../crawler/run.py'
        return self.start_process(uuid, CRAWLER, file)


    def start_parser(self):
        '''
        Use this method in REST_API to create and launch Parser.
        '''
        uuid = str(uuid4())[:25] #  create internal uuid for parser
        file = '../parser/run.py'
        return self.start_process(uuid, PARSER, file)


    def task_callback(self, uuid, result):
        '''
        Callback method that writes to log file and cleans
        self.objects dict after process termination.
        '''
        # after process creation
        if result == IN_PROCESS:
            self.objects[uuid]['status'] = IN_PROCESS
            self.write_log_file(self.objects[uuid]['internal_id'], uuid, self.objects[uuid]['type'], result)
            return
        # in case of process finish
        if self.objects[uuid]['status'] != TERMINATED:  # if process failed
            self.write_log_file(self.objects[uuid]['internal_id'], uuid, self.objects[uuid]['type'], result)
        del self.objects[uuid]


    def start_process(self, _uuid, object_type, file_to_run):
        '''
        Method that creates a separate thread which runs crawler/parser.
        '''
        callback = self.task_callback
        res = create_subprocess(_uuid, file_to_run, callback)  # returns (bool,pid)

        # response params
        obj_type = object_type
        pid = res[1]
        status = None

        if res[0]:
            status = STARTED
        else:
            status = FAILED

        # add to hashes
        process_info = {'type': obj_type, 'status': status, 'internal_id': pid}
        self.objects[_uuid] = process_info

        self.write_log_file(pid, _uuid, obj_type, status)
        # return {_uuid: response}
        return self.json_response(_uuid)


    def terminate_process(self, uuid):
        '''
        Forcely terminate process with given UUID.
        '''
        try:
            self.objects[uuid]
        except KeyError:
            return (False, {'UUID': uuid, 'status': FAILED})
        else:
            _id = self.objects[uuid]['internal_id']
            obj_type = self.objects[uuid]['type']

            if not kill_process(uuid):
                self.write_log_file(_id, uuid, obj_type, FAILED)
                return (False, {'UUID': uuid, 'status': FAILED})
            else:
                self.write_log_file(_id, uuid, obj_type, TERMINATED)
                self.objects[uuid]['status'] = TERMINATED
                return (True, {'UUID': uuid, 'status': TERMINATED})

    
    def get_crawlers(self):
        res = {}
        for k in self.objects:
            if self.objects[k]['type'] == CRAWLER:
                res[k] = self.objects[k]
    
        return res


    def get_parsers(self):
        res = {}
        for k in self.objects:
            if self.objects[k]['type'] == PARSER:
                res[k] = self.objects[k]
    
        return res


    def init_logger(self):
        '''
        Initialize log file.
        '''
        logger = logging.getLogger('controller_app')
        logger.setLevel(logging.INFO)

        # create a file handler
        handler = logging.FileHandler('logfile.log')
        handler.setLevel(logging.INFO)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(handler)
        return logger


    def write_log_file(self, pid, uuid, type_proc, action):
        '''
        Invoke this method to write in log file.
        '''
        self.logger.info('thread: {0} uuid: {1} type: {2} {3}'.format(pid, uuid, type_proc, action))
        self.change_status_in_db(uuid, action)


    def db_setup(self):
        '''
        Connect to MongoDB collection 'Crawler'.
        '''
        db = Data_base("crawler").connect_db()
        collection = db["crawler"]
        return collection
        # pass


    def change_status_in_db(self, uuid, status):
        '''
        With given UUID change crawler status in MongoDB.
        '''
        self.db.update_one({'_id': ObjectId(uuid)}, {"$set": {"status": status}})
        # pass


    def json_response(self, uuid):
        '''
        Response for requests in Flask.
        '''
        try:
            self.objects[uuid]
        except KeyError:
            print('Object doesnt exist')
        else:
            # return {'crawler_id': uuid, 'internal_id': self.objects[uuid]['internal_id'],
            #         'status': self.objects[uuid]['status']}
            response = self.objects[uuid]
            response['crawler_id'] = uuid
            # get search_condition from db
            cursor = self.db.find({"_id":ObjectId(uuid)})
            search_condition = cursor[0]['search_condition']
            response['search_condition'] = search_condition
            return response


    


          































