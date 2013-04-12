import os
import time

from .job import Job
from . import worker

jobs = {}
event_loop = None

def set_event_loop(new_event_loop):
  global event_loop
  event_loop = new_event_loop

def new(jobpack):

  job = Job(jobpack)
  store_with_unique_name(job)
  handle = event_loop.call_soon(start, job)
  return job

def get(name):
  return jobs.get(name)

def store_with_unique_name(job):
  while True:
    name = "{}@{}".format(job.prefix, time.time())
    if name not in jobs:
      job.name = name
      jobs[name] = job
      return name

def start(job):  
  path = job.job_dir
  for input in job.inputs:
    worker.start(
      job,
      dict(
        host       = "localhost",
        disco_data = os.path.join(path, "data"),
        ddfs_data  = os.path.join(path, "ddfs"),
        master     = "http://localhost:8989",
        taskid     = 0,
        jobfile    =  job.jobfile_path, 
        mode       = "map", 
        jobname    = job.name, 
        disco_port =  8989,
        put_port   = 8990
      ),
      input,
    )

