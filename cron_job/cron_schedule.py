# from cron_job_interface import CronJob
from crontab import CronTab


# cron = CronJob()
# cron.add_every_minute(1, '/Library/Frameworks/Python.framework/Versions/3.6/bin/python3 /Users/irinanazarchuk/Documents/code/python/crawler-python/cron_job/cron_worker.py')
# # cron.add_every_minute(1, 'python3 cron_worker.py')
# cron.get_cron_jobs()  # or check in console for running jobs 'crontab -l'

cron  = CronTab(user=True)
job = cron.new(command='/Library/Frameworks/Python.framework/Versions/3.6/bin/python3 /Users/irinanazarchuk/Documents/code/python/crawler-python/cron_job/cron_worker.py')
job.minute.every(1) # plan job
cron.write()