import sys

from crawler import Crawler

c = Crawler(_id=sys.argv[1])
# c = Crawler(_id="5a609c5059c66de3dbcba0da")

c.setup()

c.run()
