from subprocess import PIPE
from functools import partial
import socket
import subprocess
import select
import os
import sys
import json

from disco.job import JobPack
from StringIO import StringIO

import gevent



def relative(path1, path2):
  if os.path.isfile(path1):
    path1 = os.path.dirname(path1)
  return os.path.abspath(os.path.join(path1,path2))


def readuntil(stream,term):
  b = bytearray()
  while True:
    c = stream.read(1)
    if c == term:
      return b
    else:
      b.append(c)

def readbytes(stream, count):
  return stream.read(count)
  

def recv_first_packet(stream):
 
  type = readuntil(stream, ' ')
  if type not in ("WORKER", "MSG"):
    # invalid start, child must have died
    puts("Child started with improper message\n"+type+stream.read())
    sys.exit(1)
  else:
 
    size = readuntil(stream,' ')
    payload = readbytes(stream, int(size)+1)

    if type == "WORKER":
      return (type, size, payload), recv_next_packet
    else:
      return (type, size, payload), recv_first_packet

def recv_next_packet(stream):
  return recv_packet(stream), recv_next_packet

def recv_packet(stream):

  type = readuntil(stream, ' ')
  size = readuntil(stream,' ')

  payload = readbytes(stream, int(size)+1) 
  return type, size, payload

def puts(msg, write=sys.stdout.write):
  """Displays a string for the user"""
  lines = msg.splitlines()
  write("----> " + lines.pop(0) + '\n')

  for line in lines[1:]:
    write("     ")
    write(line)
    write('\n')


def msg(type, payload):
  payload = json.dumps(payload) 
  return "{} {} {}\n".format(type, len(payload), payload)

def response(packet):
  type,size,payload = packet
  payload = json.loads(payload)
  
  if type == 'WORKER': 
    return  msg("OK", 'ok')
  elif type == 'MSG':
    puts(payload)
    return msg("OK","")
  elif type in ('ERROR','FATAL'):
    puts(payload)
    return None
  elif type == 'TASK':
    return msg('TASK', {
      "host": "broker",
      "disco_data": "/srv/disco/data",
      "master": "http://localhost:8989",

      "ddfs_data": "/srv/disco/ddfs", 
      "taskid": 0,
      "jobfile": "/srv/disco/data/broker/bb/17c0305a-c027-4e33-9f83-0662739c92c3:824:training_set:2013:01:07:00@54d:75bb0:bbea3/jobfile", 
      "mode": "reduce", 
      "jobname": "17c0305a-c027-4e33-9f83-0662739c92c3:824:training_set:2013:01:07:00@54d:75bb0:bbea3", 
      "disco_port": 8989,
      "put_port": 8990
    })
  elif type == "INPUT":
    return msg('INPUT', [u'done', [[0, u'ok', [[0, u'dir://broker/disco/broker/bb/17c0305a-c027-4e33-9f83-0662739c92c3:824:training_set:2013:01:07:00@54d:75bb0:bbea3/.disco/map-index.txt.gz']]]]]) 
  else:
    pass


def worker_stream(proc):
  packet_reader  = [recv_first_packet]

  def _(stream):
    packet, packet_reader[0] = packet_reader[0](stream)

    #print "<--" + str(packet),
    r = response(packet) 
    #print "--> {}".format(r)
    if r is not None:
      proc.stdin.write(r)
      return True
    else:
      proc.kill()
      return False


  return _

def debug_output(client):
  while 1:
    try:
      sys.stdout.write(client.recv(1024))
    except socket.error:
      break

def debug_input(client, stdin):
  client.send(stdin.readline())

def start_debug(readers, srv_socket):
  client, addr = srv_socket.accept()
  puts("Starting debug session")
  readers.clear()

  readers[client] = debug_output
  readers[sys.stdin] = partial(debug_input, client)
  return True



def open_debug_socket():

  serversocket = socket.socket(
      socket.AF_INET,
      socket.SOCK_STREAM
  )
  
  serversocket.bind(('localhost', 5665))
  serversocket.setblocking(0)
  #become a server socket
  serversocket.listen(5)
  return serversocket


def jobinfo():
  return json.dumps(dict(
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
  ))

def rawevents():
  events = [
    ["2013/03/20 12:45:20","master","New job initialized!"]
  ]

  return "\n".join([ json.dumps(e) for e in events ])

def get_results():
  return json.dumps([
    ["Job",["active", []]]
  ])

def patch_disco(store):
  from disco.core import Disco
  def submit(self, jobpack):
    store(jobpack)
    return JobPack.load(StringIO(jobpack)).jobdict['prefix']

  def request(self, url, data=None, offset=0):
    puts(url)
    if url == '/disco/ctrl/jobinfo?name=Job':
      return jobinfo()
    elif url == '/disco/ctrl/rawevents?name=Job':
      return rawevents()[offset:]
    elif url == '/disco/ctrl/get_results':
      return get_results()
    else:
      import pdb; pdb.set_trace()
      pass

  Disco.submit = submit
  Disco.request = request

def run_script(script, retreive):
  import disco
  os.environ['DISCO_HOME'] = disco.__path__[0]
  globals_ = {
    "__name__" : "__main__",
    "__file__" : script,
    "__builtins__" : __builtins__
  }
  locals_ = globals_

  exec(compile(open(script).read(), script, 'exec'), globals_, locals_)
  return retreive

def main():
  env = os.environ.copy()
  script = sys.argv[1]

  blah = []
  patch_disco(blah.append)

  job = run_script(script, blah.pop)

  proc  = subprocess.Popen(
    ['sh', '-c', relative(__file__, 'minions/__init__.py')],
    stdin=PIPE,
    stdout=PIPE,
    stderr=PIPE,
    env=env
  )

  inputs = {
    proc.stderr: worker_stream(proc),
  }
  inputs[open_debug_socket()] = partial(start_debug, inputs)

  rc = proc.poll()
  while rc is None:
    readers,writers,errs= select.select(inputs.keys(),[proc.stdin],[])
    for reader in readers:
      keep = inputs[reader](reader)
      if not keep:
        del inputs[reader]
    rc = proc.poll()
  
  print proc.stderr.read(1000)
  pass


if __name__ == "__main__":
  main()
