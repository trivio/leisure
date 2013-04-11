import os
import tempfile
import stat
import zipfile
from StringIO import StringIO

from . import worker

def start(jobpack, raw_jobpack, callback):
  name = jobpack.jobdict['prefix']
  
  path = extract_jobhome(jobpack.jobhome)
  job_file_path = os.path.join(path, 'jobfile')
  open(job_file_path,'w').write(raw_jobpack)

  worker_path = os.path.join(path, jobpack.jobdict['worker'])
  st = os.stat(worker_path)
  os.chmod(worker_path, st.st_mode | stat.S_IEXEC)

  jobpack.jobdict['input']
  for input in jobpack.jobdict['input']:

    worker.start(
      path,
      worker_path,
      dict(
        host       = "localhost",
        disco_data = os.path.join(path, "data"),
        ddfs_data  = os.path.join(path, "ddfs"),
        master     = "http://localhost:8989",
        taskid     = 0,
        jobfile    =  job_file_path, 
        mode       = "map", 
        jobname    = name, 
        disco_port =  8989,
        put_port   = 8990
      ),
      input,
      callback
    )


def extract_jobhome(jobhome):
  """Extract job to a tempporary directory and returns it's path"""

  z = zipfile.ZipFile(StringIO(jobhome), 'r')
  path = tempfile.mkdtemp()
  z.extractall(path)
  return path

