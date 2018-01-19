import sys

from crawler import Crawler

c = Crawler(_id=sys.argv[1])
# c = Crawler(_id="5a5f74b13e7d66747f062e2f")

c.setup()

c.run()
