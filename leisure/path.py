import os, errno
def relative(path1, path2):
  if os.path.isfile(path1):
    path1 = os.path.dirname(path1)
  return os.path.abspath(os.path.join(path1,path2))



def makedirs(path):
  try:
      os.makedirs(path)
  except OSError as exc:
    if exc.errno == errno.EEXIST and os.path.isdir(path):
        pass
    else: raise
  return path

def ensure_dir(path):
  return makedirs(os.path.dirname(path))
