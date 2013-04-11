from __future__ import absolute_import

import json
import os
from StringIO import StringIO
from urlparse import urlparse, parse_qs
import threading
from functools import partial


import disco
from disco.job import JobPack
from disco.core import Disco


from .io import puts
from .   import event_loop
from .   import job_control

def run_script(script):
  loop = start_event_loop()
  try:
    patch_disco(loop)

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

def patch_disco(event_loop):
  job_info = {}
  def submit(self, raw_jobpack):
    jobpack = JobPack.load(StringIO(raw_jobpack))    
    handle = event_loop.call_soon(job_control.start, jobpack, raw_jobpack, ready)

    name = "{}@{}".format(jobpack.jobdict['prefix'], id(handle))

    job_info[name] = dict(
      timestamp = "2013/04/07 23:00:25",
      active    = "active",
      mapi      = [0,1,0,0],
      redi      = [1,0,0,0],
      reduce    = True,
      results   = [],
      inputs    = [
        "http://aws-publicdatasets.s3.amazonaws.com/common-crawl/crawl-002/2009/09/17/0/1253228619531_0.arc.gz?Signature=tNmfzENEA2RUByEMiLTgC8xYUOA%3D&Expires=1365608001&AWSAccessKeyId=AKIAJ6PF5PWHFME4GA5A"
      ],
      worker    = "app/minions/__init__.py",
      hosts     = [
        "localhost"
      ],
      owner     = "u23541@09658551-c6a7-450a-9177-1256951b042f"
    )
    return name


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

  # We're local so reduce lag by polling faster... 
  Disco._wait = Disco.wait
  def wait(self,  jobname, poll_interval=.01, timeout=None, clean=False, show=None):
    return self._wait(jobname, poll_interval, timeout, clean, show)
  Disco.wait = wait

  Disco.submit = submit
  Disco.request = request
  

  def ready(status):
    for k,v in job_info.items():
      v["active"] = status

  def jobinfo(name):
    return json.dumps(job_info[name])

  def rawevents(name):
    events = [
      ["2013/03/20 12:45:20","master","New job initialized!"]
    ]

    return "\n".join([ json.dumps(e) for e in events ])

  def get_results():
    return json.dumps([
      (job_id, [info["active"], []] )for job_id, info in job_info.items()
    ])

    return json.dumps([
      ["Job",["active", []]]
    ])
