#!/usr/bin/python

# import parser
# import cron
import time
import random
from threading import Thread
import logging
from uuid import uuid4
from .sub_procs import procs, create_subprocess, kill_process




# object constants
CRAWLER = 'CRAWLER'
PARSER = 'PARSER'
GRAPH_MAKER = 'GRAPH_MAKER'

# status constants
STARTED = 'STARTED'
FINISHED = 'FINISHED'
TERMINATED = 'TERMINATED'
FAILED = 'FAILED'
IN_PROCESS = 'IN PROCESS'



class Controller:

    '''
    Controller class that gives you an opportunity to 
    manipulate such objects as Crawler, Parser and GraphMaker. 
    '''

    def __init__(self, environ=None):
        self.environ = environ
        self.objects = {}  # key - uuid, value - dict with object data (type, status, internal_id)
        self.logger = self.init_logger()


    def set_up(self):
        pass


    def start(self):
        pass


    def status(self): 
        '''
        Get all content of self.objects HashMap
        '''
        return self.objects


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
        uuid = str(uuid4())[:25] #  create internal uuid for parser
        file = '../parser/run.py'
        return self.start_process(uuid, PARSER, file)


    def task_finished(self, uuid, result):
        '''
        Callback method that writes to log file and cleans
        self.objects HashMap after process termination.
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
        callback = (self, self.task_finished)
        res = create_subprocess(_uuid, file_to_run, callback)  # returns (bool,pid)

        # response params
        obj_type = object_type
        pid = res[1]
        status = None

        if res[0]:
            status = STARTED
        else:
            status = FAILED

        self.write_log_file(pid, _uuid, obj_type, status)

        # add to hashes
        response = {'type': obj_type, 'status': status, 'internal_id': pid}
        self.objects[_uuid] = response

        return {_uuid: response}  


    def terminate_process(self, uuid):
        '''
        Forcely terminate process with particular UUID.
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


    


          































