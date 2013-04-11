import os
def relative(path1, path2):
  if os.path.isfile(path1):
    path1 = os.path.dirname(path1)
  return os.path.abspath(os.path.join(path1,path2))
