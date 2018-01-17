import sys

from crawler import Crawler

c = Crawler(_id=sys.argv[1])

c.setup()

c.run()
