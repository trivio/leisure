import os
import sys
from subprocess import PIPE
import subprocess
import json

from .io import puts, indent, readuntil, readbytes
from .event_loop import add_reader, remove_reader
from .path import relative

def start(task):
  puts("Starting job in {}".format(task.job_dir))
  indent("inputs {}".format(task.input))
  env = os.environ.copy()
  proc  = subprocess.Popen(
    [task.worker_path],
    stdin=PIPE,
    stdout=PIPE,
    stderr=PIPE,
    cwd=task.job_dir,
    env=env
  )

  add_reader(proc.stderr, worker_stream(proc, task), proc.stderr)


def worker_stream(proc, task):
  """
  Returns a function bound to the supplied proc suitable for interpreting
  the disco work protocol.
  """

  packet_reader  = [recv_first_packet]

  def _(stream):
    packet, packet_reader[0] = packet_reader[0](stream)

    #print "<--" + str(packet),
    r = response(proc, task, packet)
    #print "--> {}".format(r)
    if r is not None:
      proc.stdin.write(r)
 
  return _

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

def msg(type, payload):
  payload = json.dumps(payload) 
  return "{} {} {}\n".format(type, len(payload), payload)

def done(proc):
  proc.kill()
  remove_reader(proc.stderr)
  return msg("OK", "ok")  

def response(proc, task, packet):
  type,size,payload = packet
  payload = json.loads(payload)
  
  if type == 'WORKER': 
    return  msg("OK", 'ok')
  elif type == 'MSG':
    puts(payload)
    return msg("OK","")
  elif type in ('ERROR','FATAL'):
    # todo: fail, the task
    task.job.status = "dead"
    done(proc)
    puts("{}\n{}".format(type, payload))
    return None
  elif type == 'TASK':
    return msg('TASK',task.info())

  elif type == "INPUT":
    return msg('INPUT', [
      u'done', [
        [0, u'ok', [[0, task.input]]]
      ]
    ]) 
  elif type == "OUTPUT":
    puts("{} {} {}".format(*packet))
    task.add_output(*payload)

    return  msg("OK", 'ok')
  elif type == "DONE":
    task.done()
    return done(proc)
  else:
    raise RuntimeError("Uknown message type '' received".format(type))
