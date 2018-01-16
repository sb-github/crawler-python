import time
import random
import sys


class MockObj:
    ''' 
    Mock object for testing. Keeps tracking 
    of the number of class instances
    '''

    _count = 0

    @classmethod
    def count(cls):
        MockObj._count += 1
        return MockObj._count

    def __init__(self, name, **options):
        self.name = name
        self.id = self.count()
        if 'delay' in options:
            self.delay = options['delay']
        else:
            self.delay = 0

    def task(self, uuid):
        time.sleep(self.delay)
        with open('{}.txt'.format(uuid), 'w') as f:
            text = "Created object {}\n".format(self.id)
            f.write(text)
            i = 0
            while i <= self.delay:
                time.sleep(1)
                f.write(str(i)+ '\n')
                f.flush()
                i += 1


mock = MockObj(random.randrange(100), delay=3)
mock.task(sys.argv[1])