import sys
from parser import Parser_vacancy

pars = Parser_vacancy(_id=sys.argv[1])

pars.run()