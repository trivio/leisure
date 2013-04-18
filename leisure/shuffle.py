import os
from gzip import GzipFile
from itertools import groupby, chain

from . import disco
from .path import makedirs

def combine_tasks(data_root, job, mode, task_results):
  key_func = lambda a: a[0]

  for node, urls in groupby(sorted(task_results, key=key_func), key_func):
    dir_urls = [url for node, url in urls]
    #print node, dir_urls
    yield combine_tasks_node(node, data_root, job,  mode, dir_urls)

def combine_tasks_node(host, data_root, job_name, mode, dir_urls):
  job_home = disco.job_home(job_name, os.path.join(data_root, host))

  results_home = os.path.join(job_home, ".disco")
  
  job_link = disco.job_home(job_name, os.path.join(host, "disco", host))

  results_link = os.path.join(job_link, ".disco")
  part_dir = "partitions-{}".format(disco.timestamp())

  part_path = os.path.join(results_home, part_dir)
  part_url = os.path.join("disco://", results_link,  part_dir)
  index_file = "{}-index.txt.gz".format(mode)
  index_path = os.path.join(results_home, index_file)
  index_url =  os.path.join("dir://", results_link,  index_file)

  makedirs(part_path)
  
  index = merged_index(dir_urls, data_root, (part_path, part_url))
  write_index(index_path, index)
  return index_url


def merged_index(dir_urls, data_root, part_info):
  return set(chain.from_iterable(
    process_task(dir_url, data_root, part_info)
    for dir_url in dir_urls
  )) 

def process_task(dir_url, data_root, part_info):

  task_path = disco.disco_url_path(dir_url)
  for line in open(os.path.join(data_root, task_path)):
    e = line.split()
    yield process_url(e, data_root, part_info)

def process_url((label, url), data_root, (part_path, part_url)):
  """
  Given a lable and a url merge the data in the url if
  it is a local result (i.e. starts with "part://") and return
  the url to the new part file. Otherwise returns the label and
  url unmodified.

  """
  if not url.startswith('part://'):
    return "{} {}\n".format(label, url)
  else:
    part_file = "part-{}".format(label)
    part_src  = os.path.join(data_root,  disco.disco_url_path(url))
    part_dst  = os.path.join(part_path,  part_file)

    concat(part_src, part_dst)
    return "{} {}/{}\n".format(label, part_url, part_file)

def write_index(filename, lines):
  """
  Atomic write of index

  Output lines to a temporary file renaming it when done. 
  Returns the name of the file written
  """
  

  tmp_path = "{}-{}".format(filename, disco.timestamp())
  output = GzipFile(tmp_path, 'w')
  #output = open(tmp_path, 'w')
  output.writelines(lines)
  output.close()
  os.rename(tmp_path, filename)
  return filename




def concat(src_path, dst_path):
  src = open(src_path, 'rb')
  dst = open(dst_path, 'ab')

  while True:
    chunk = src.read(524288)
    if chunk:
      dst.write(chunk)
    else:
      break

  src.close()
  dst.close()
