from __future__ import absolute_import

from datetime import datetime
import os
import stat


import zipfile
from StringIO import StringIO

from disco.job import JobPack
import leisure

class Job(object):
  def __init__(self, jobpack):
    self.jobpack = JobPack.load(StringIO(jobpack))
    self.host = "localhost"
    self.data_root = os.environ['DISCO_DATA']

    self.name = "{}@{}".format(self.prefix, leisure.disco.timestamp())


    
    self.home = os.path.join(
      self.host, 
      leisure.disco.hex_hash(self.name), 
      self.name
    )
    self.job_dir = extract_jobhome(
      os.path.join(self.data_root, self.home),
      self.jobpack.jobhome
    )

    self.save_jobfile(jobpack)
    self.ensure_worker_executable()
    self.results = []
    self.status = "active"
  

  @property
  def prefix(self):
    return self.jobpack.jobdict['prefix']

  @property
  def inputs(self):
    return self.jobpack.jobdict['input']

  @property
  def worker(self):
    return self.jobpack.jobdict['worker']

  @property
  def worker_path(self):
    return os.path.join(self.job_dir, self.worker)

  @property
  def jobfile_path(self):
    return os.path.join(self.job_dir, "jobfile")

  @property
  def nr_reduces(self):
    return self.jobpack.jobdict['nr_reduces']
    
  @property
  def has_map_phase(self):
    """Return true if the job has a map phase"""
    return self.jobpack.jobdict['map?']


  @property
  def has_reduce_phase(self):
    """Return true if the job has a map phase"""
    return self.jobpack.jobdict['reduce?']

  def info(self):
    return dict(
      timestamp = str(datetime.utcnow()),
      active    = self.status,
      mapi      = [0,1,0,0],
      redi      = [1,0,0,0],
      reduce    = True,
      results   = self.results,
      inputs    = self.inputs,
      worker    = self.worker,
      hosts     = [
        "localhost"
      ],
      owner     = "u23541@09658551-c6a7-450a-9177-1256951b042f"
    )

  def save_jobfile(self, jobpack):
    open(self.jobfile_path,'w').write(jobpack)

  def ensure_worker_executable(self):
    """Makes the worker executable"""

    worker_path = os.path.join(self.job_dir, self.worker)
    st = os.stat(worker_path)
    os.chmod(worker_path, st.st_mode | stat.S_IEXEC)


def extract_jobhome(path, jobhome):
  """Extract job to a tempporary directory and returns it's path"""

  z = zipfile.ZipFile(StringIO(jobhome), 'r')
  z.extractall(path)
  return path