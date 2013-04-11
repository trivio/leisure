import sys

def puts(msg, write=sys.stdout.write):
  """Displays a string for the user"""
  lines = msg.splitlines()
  write("----> " + lines.pop(0) + '\n')

  for line in lines:
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
