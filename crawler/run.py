'''import sys
from crawler import Crawler

c = Crawler(_id=sys.argv[1])

c.setup()

c.run()
'''

import sys

print(sys.path)

sys.path.append('..')

print(sys.path)

import pars.parse

print(pars.parser.foo)