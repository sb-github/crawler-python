#!/usr/bin/python

from .crawler import Crawler
from .sub_procs import create_subprocess
# import parser
# import cron
import time
import random
# from .testing import MockObj


class Controller:

    def __init__(self, environ=None):
        self.environ = environ
        self.objects = {}

    def set_up(self):
        pass

    def start(self):
        pass

    def status(self, proc_id):
        pass

    def shut_down(self):
        pass

    def start_crawler(self, uid):
        crw = Crawler(uid)
        crw.setup()
        crw.run()
        res = create_subprocess('run.py', uid)
        if res:
            self.objects[uid] = {'type': 'crawler', 'status': 'active'}
        else:
            return {'type': 'crawler', 'status': 'failed', 'error_description': 'text'}

        # mock = MockObj(random.randrange(100), delay=0)
        # pid = mock.id
        # self.objects[pid] = mock
        # mock.task()

        return {uid: self.objects[uid]}


    def get_crawler_by_id(self, pid):  # mock
        
        try:
            self.objects[pid]
        except KeyError:
            return False
        else:
            obj_name = self.objects[pid].name
            return {'name': obj_name}



    def stop_crawler(self, pid):  # mock
    
        try:
            self.objects[pid]
        except KeyError:
            return False
        else:
            del self.objects[pid]
            return {'status': 'disabled'}


    def get_crawlers(self): # mock
        # res = {}
        # for k, v in self.objects.items():
        #     res[k] = v.name
        # return res

        res = {}
        for key in self.objects:
            if self.objects[key]['type'] == 'crawler':
                res[key] = self.objects[key]
        return res


    






























