from crontab import CronTab


class CronJob:

    def __init__(self):
        self.cron = CronTab(user=True)
        self.cron.render()

    def add_every_minute(self, minutes, command, user=None, comment=None, environment=None):
        cron_job = self.cron.new(command=command, user=user, comment=comment)
        cron_job.minute.every(minutes)
        cron_job.enable()
        self.cron.write()

    def add_every_hour(self, hours, command, user=None, comment=None, environment=None):
        cron_job = self.cron.new(command=command, user=user, comment=comment)
        cron_job.minute.on(0)
        cron_job.hour.every(hours)
        # cron_job.hour.during(0,23)
        cron_job.enable()
        self.cron.write()

    def add_every_day(self, days, command, user=None, comment=None, environment=None):
        cron_job = self.cron.new(command=command, user=user, comment=comment)
        cron_job.minute.on(0)
        cron_job.hour.every(0)
        cron_job.enable()
        self.cron.write()

    def get_cron_jobs(self):
        for job in self.cron:
            print(job)
        
    def get_job(self, comment):
        for job in self.cron:
            if job.comment == comment:
                return job

    def remove_job(self, comment):
        job = self.get_job(comment)
        self.cron.remove(job)
        self.cron.write()
        # cron.remove_all(comment=comment)
                

    

cron = CronJob()
cron.add_every_minute(minutes=2, command='python3 cron_worker.py')


    
