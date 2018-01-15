import time


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

    def task(self):
        time.sleep(self.delay)
        with open('test.txt', 'a') as f:
            text = "Created object {}\n".format(self.id)
            f.write(text)