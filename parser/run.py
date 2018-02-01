#!/usr/bin/python3
from parser_vacancy import Parser_vacancy
import datetime


p = Parser_vacancy()
p.run()
# with open('/Users/irinanazarchuk/Documents/code/python/crawler_docker/crawler-python/parser/info.txt', 'a') as outFile:
#     outFile.write('\n' + str(datetime.datetime.now()))