from cron_job_interface import CronJob


cron = CronJob()
cron.add_every_minute(minutes=20, command='/Library/Frameworks/Python.framework/Versions/3.6/bin/python3 /Users/irinanazarchuk/Documents/code/python/crawler-python/parser/run.py', comment='parser')
# cron.add_every_minute(1, 'python3 cron_worker.py')
cron.get_cron_jobs()  # or check in console for running jobs 'crontab -l'
# cron.remove_job('parser')
