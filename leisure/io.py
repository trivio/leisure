import sys
from colorama import init, Fore, Back
init()

def puts(msg, write=sys.stdout.write, fore=None, back=None):
  """Displays a string for the user"""
  if fore:
    fore = getattr(Fore, fore.upper())
  else:
    fore = ''


  lines = msg.splitlines()
  write("----> " + fore + lines.pop(0) + '\n')

  for line in lines:
    write("     ")
    write(line)
    write('\n')
  write(Fore.RESET)

def indent(msg, write=sys.stdout.write):
  for line in msg.splitlines():
    write("     ")
    write(line)
    write('\n')

def readuntil(stream,term):
  b = bytearray()
  while True:
    c = stream.read(1)
    if c in (term,''):
      return b
    else:
      b.append(c)

def readbytes(stream, count):
  return stream.read(count)
