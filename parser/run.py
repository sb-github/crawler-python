#!/usr/bin/python3
from parser import Parser_vacancy
import datetime


# p = Parser_vacancy()
# p.run()
with open('info.txt', 'a') as outFile:
    outFile.write('\n' + str(datetime.datetime.now()))