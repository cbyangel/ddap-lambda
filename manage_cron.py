from crontab import CronTab

cron  = CronTab(user=True)

job  = cron.new(command='cd /data2/ddap && /data2/ddap/conda/envs/ddap-batch/bin/python batch.py', comment='ddap daily batch')
job.hour.on(12)
job.minute.on(0)

# test run
# job_standard_output = job.run()

# check how many runs on day
job.frequency_per_day()

if job.valid:
    job.enable(True)
    # write system crontab
    cron.write()

# check added crons
print(cron.crons)

# drop all (need write)
cron.remove_all()
