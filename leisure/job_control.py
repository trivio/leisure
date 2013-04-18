import os
import time
from collections import namedtuple

from .job import Job
from . import worker
from .task import Task

import leisure

jobs = {}
def get(name):
  return jobs.get(name)

def all():
  return jobs.values()

def active():
  return filter(lambda j: j.status == "active", all())

def ready():
  return filter(lambda j: j.status == "ready", all())

def dead():
  return filter(lambda j: j.status == "dead", all())


event_loop = None

def set_event_loop(new_event_loop):
  global event_loop
  event_loop = new_event_loop

def new(jobpack):

  job = Job(jobpack)
  store_with_unique_name(job)
  handle = event_loop.call_soon(map_reduce, job)
  return job


def store_with_unique_name(job):
  jobs[job.name] = job
  return job.name 
  while True:
    name = "{}@{}".format(job.prefix, time.time())
    if name not in jobs:
      job.name = name
      jobs[name] = job
      return name

def map_reduce(job):

  def _reduce(inputs):
    return reduce(inputs, job, _finished)

  def _finished(results):
    job.results.extend(results)
    job.status = "ready"

  map(job.inputs, job, _reduce)


def map(inputs, job, cb):
  if not job.has_map_phase:
    return event_loop.call_soon(cb, inputs)
    #return inuts
  else:
    return run_phase(map_inputs(inputs), "map", job, cb)

def map_inputs(inputs):
  # preferred_host = leisure.disco.preferred_host
  # def case(input):
  #   if isinstance(input, list):
  #     return [ (i, preferred_host(input)) for i in input ]
  #   else:
  #     return [(input, preferred_host(input))]

  # return list(enumerate([ case(input) for input in inputs ]))


  if not hasattr(inputs, '__iter__'):
    inputs = [inputs]

  return inputs

def reduce(inputs, job, cb):
  
  if not job.has_reduce_phase:
    return event_loop.call_soon(cb, inputs)
  else:
    return run_phase(reduce_inputs(inputs, job.nr_reduces), "reduce", job, cb)

def reduce_inputs(inputs, n_red):
  return inputs
  hosts = usort([ 
    leisure.disco.preferred_host(input)
    for input in inputs
  ])

  num_hosts = len(hosts)
  if num_hosts == 0:
    return []
  else:
    hosts_d = dict(enumerate(hosts))
    return [
      (task_id, [(inputs, hosts_d[task_id % n_red])])
      for task_id in range(num_hosts) 
    ]

def usort(inputs):
  return sorted(set(inputs))

def results(job, mode, local_results, global_results, **state):

  res = leisure.shuffle.combine_tasks(
    job.data_root, 
    job.name, mode, 
    local_results
  )
  

  return sorted(set(global_results).union(res))
  

def run_phase(inputs, mode, job, cb):
  if not inputs:
    cb(inputs)

  path = job.job_dir

  state = dict(
    mode           = mode,
    job            = job,
    cb             = cb, 
    outstanding    = len(inputs),
    local_results  = [],
    global_results = []
  )

  for id, input in enumerate(inputs):
    task = Task(id, job, input, mode)
    task.on('done', on_task_done,  state)
    worker.start(task)

def on_task_done(task, state):
  
  local_result,global_results = task.results()
  if local_result:
    state['local_results'].append((task.host, local_result))
  
  state["global_results"].extend(global_results)

  state["outstanding"] -= 1
  if state["outstanding"] == 0:
    state["cb"](results(**state))
