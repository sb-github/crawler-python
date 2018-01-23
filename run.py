import sys
from crawler import Crawler

# uid = "5a58bb84189bf2ae9b229efc"
cr = Crawler(_id=sys.argv[1])

cr.setup()

cr.run()

# print(cr.vacancies_dict)

