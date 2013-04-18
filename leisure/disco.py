from __future__ import absolute_import

import json
import os
from StringIO import StringIO
from urlparse import urlparse, parse_qs
import threading
import re
from functools import partial
import hashlib
from datetime import datetime
import time

import disco
from disco.core import Disco

from .io import puts
from .   import event_loop
from .   import job_control
from .   import server

disco_url_regex = re.compile(".*?://.*?/disco/(.*)")
preferred_host_re = re.compile("^[a-zA-Z0-9]+://([^/:]*)") 

def run_script(script, data_root):
  loop = start_event_loop()
  job_control.set_event_loop(loop)
  try:
    patch_disco()
    host,port = server.start(loop)

    os.environ['DISCO_HOME'] = disco.__path__[0]
    os.environ['DISCO_DATA'] = data_root

    os.environ['DISCO_PORT'] = str(port)
    os.environ['DDFS_PUT_PORT'] = str(port)


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


def disco_url_path(url):
  return disco_url_regex.match(url).group(1)

def job_home(job_name, root):
  return  os.path.join(root, hex_hash(job_name), job_name)

def job_url(host, job_name):
  return os.path.join("disco", host, hex_hash(job_name), job_name)

def hex_hash(path):
  """
  Return the first 2 hex digits of the md5 of the given path.
  Suitable for creating sub dirs to break up a large directory
  """

  return hashlib.md5(path).hexdigest()[:2]

def preferred_host(url):

  m = preferred_host_re.search(url)
  if m:
    return m.group(1)


def timestamp(dt=None):
  """
  Return a timestamp in the format of hex(megasecconds)-hex(seconds)-hex(microseconds)

  The timestamp should be monotonically increasing and hence usable as a uuid
  """
  if dt is None:
    dt = datetime.utcnow()
  mega, seconds = map(int, divmod(time.mktime(dt.timetuple()), 10**6))
  return "{:x}-{:x}-{:x}".format(mega, seconds, dt.microsecond) 

