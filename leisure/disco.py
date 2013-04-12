from __future__ import absolute_import

import json
import os
from StringIO import StringIO
from urlparse import urlparse, parse_qs
import threading
from functools import partial


import disco

from disco.core import Disco


from .io import puts
from .   import event_loop
from .   import job_control

def run_script(script):
  loop = start_event_loop()
  job_control.set_event_loop(loop)
  try:
    patch_disco()

    os.environ['DISCO_HOME'] = disco.__path__[0]
    globals_ = {
      "__name__" : "__main__",
      "__file__" : script,
      "__builtins__" : __builtins__
    }
    locals_ = globals_
    exec(compile(open(script).read(), script, 'exec'), globals_, locals_)

  finally:
    loop.stop()


def start_event_loop():
  event = threading.Event()
  ret = [] 

  def _():
    ev = event_loop.current_event_loop()
    ret.append(ev)
    # wake up our parent thread
    event.set()
    ev.run()

  threading.Thread(target=_).start()
  event.wait()
  return ret[0]

def patch_disco():
  Disco._wait = Disco.wait
  Disco.wait = wait
  Disco.submit = submit
  Disco.request = request

def submit(self, jobpack):
  return job_control.new(jobpack).name

def request(self, url, data=None, offset=0):
  #puts(url)
  url = urlparse(url)
  # unwrap query

  args = dict([ (k,v[0]) for k,v in parse_qs(url.query).items()])
  path = url.path

  if path == '/disco/ctrl/jobinfo':
    return jobinfo(**args)
  elif path == '/disco/ctrl/rawevents':
    return rawevents(**args)[offset:]
  elif path == '/disco/ctrl/get_results':
    return get_results()
  else:
    raise RuntimeError("Unexpected url {}".format(url))
  

def wait(self,  jobname, poll_interval=.01, timeout=None, clean=False, show=None):
  # We're local so reduce lag by polling faster... 
  return self._wait(jobname, poll_interval, timeout, clean, show)
  

def jobinfo(name):
  job = job_control.get(name)
  return json.dumps(job.info())

def rawevents(name):
  events = [
    ["2013/03/20 12:45:20","master","New job initialized!"]
  ]

  return "\n".join([ json.dumps(e) for e in events ])

def get_results():
  jobs = job_control.jobs.values()
  return json.dumps([
    (job.name, [job.status, job.results]) for  job in jobs
  ])

 