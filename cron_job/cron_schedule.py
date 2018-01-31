#!/usr/bin/python3
'''
This module creates CronJob instance and sets 
command for cron to execute with a particular schedule.

To check running cron jobs use get_cron_jobs() method 
or type into console $ crontab -l.

To stop cron job with particular comment use remove_job(comment)
or type into console $ crontab -e and clear vi file.

To stop all running cron jobs use remove_all_jobs() method.
'''

from cron_job_interface import CronJob


cron = CronJob()
# cron.add_every_minute(minutes=20, command='/Library/Frameworks/Python.framework/Versions/3.6/bin/python3 /Users/irinanazarchuk/Documents/code/python/crawler_docker/crawler-python/parser/run.py', comment='parser')
# cron.add_every_minute(minutes=20, command='/Library/Frameworks/Python.framework/Versions/3.6/bin/python3 /Users/irinanazarchuk/Documents/code/python/crawler_docker/crawler-python/graph_maker/run.py', comment='graph_maker')
cron.add_every_minute(minutes=20, command='python3 ../parser/run.py', comment='parser')
cron.add_every_minute(minutes=20, command='python3 ../graph_maker/run.py', comment='graph_maker')
cron.get_cron_jobs()

