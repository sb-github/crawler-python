
import datetime


with open('info.txt', 'a') as outFile:
    outFile.write('\n' + str(datetime.datetime.now()))
