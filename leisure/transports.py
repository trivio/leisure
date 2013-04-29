import fcntl
import os
import socket
import errno
from collections import deque

from .event_emmiter import EventEmmiter
class Socket(EventEmmiter):
  def __init__(self,address, delegate=None):
    self.address = address
    self.delegate = delegate
    self.event_loop = None
    self.read_buffer_size = 4096
    self.write_buffer = deque()
    self.closing = False

  def listen(self,  backlog, event_loop = None):
    """Listen for incoming connections on this port.

     backlog - the maximum number of queued connectinos

     event_loop - the event_loop that will monitor this port for
               incomming connections. Defaults to the
               current_event_loop() if none is specified.  
    """

    if type(self.address) == tuple:
      serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM )
      socket_path = None
    else:
      socket_path = self.address
      serversocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM )

      if os.path.exists(socket_path):
        # possible stale socket let's see if any one is listning
        err = serversocket.connect_ex(socket_path)
        if err == errno.ECONNREFUSED:
          os.unlink(socket_path)
        else:
          serversocket._reset()
          raise RuntimeError("Socket path %s is in use" % socket_path )


    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serversocket.bind(self.address)

    if socket_path: # ensure the world can read/write this socket
      os.chmod(socket_path, 666)

    serversocket.listen(backlog)
    serversocket.setblocking(0)

    self._socket = serversocket
    self.listening = True
    self.connected = True

    if event_loop is None:
      event_loop = current_event_loop()

    event_loop.add_reader(self._socket, self.new_connection, self._socket)
    self.event_loop = event_loop
    return self._socket.getsockname()

  def new_connection(self, srv_socket):
    client, addr = srv_socket.accept()
    new_socket = Socket(addr, self.delegate)
    new_socket.connection_accepted(client, self.event_loop)
    self.fire("accept", new_socket)

  def connection_accepted(self, socket, event_loop):
    self._socket = socket
    self.event_loop = event_loop
    self.connected = True
    self.event_loop.add_reader(socket, self.can_read, socket)

  def close(self):
    self.closing = True
    if self._socket:
      self.event_loop.remove_reader(self._socket)
      #self._socket = None
      #self.fire('closed', self)


  def can_read(self, client):

    while True:
      try:
        buf = bytearray(self.read_buffer_size)
        mem = memoryview(buf)
        bytes = client.recv_into(buf)
        if bytes > 0:
          self.fire('data', mem[:bytes])
        else:
          self.close()


      except socket.error,e:
        if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
          # other end of the socket is full, so
          # ask the runLoop when we can send more
          # data

          break
        else:
          # if we receive any other socket
          # error we close the connection
          # and raise and notify our delegate

          #self._reset()
          #self.delegate.onError(self, e)
          self.fire('error', e)
          self.event_loop.remove_reader(client)

  def write(self, data):
    self.write_buffer.append(data)
    self.event_loop.add_writer(self._socket, self.can_write)

  def can_write(self):
    while self.write_buffer:
      sent = self._socket.send(self.write_buffer[0])
      if sent == len(self.write_buffer[0]):
        self.write_buffer.popleft()
      else:
        self.write_buffer[0] = buffer(self.write_buffer[0], sent)
        break

    if not self.write_buffer:
      self.event_loop.remove_writer(self._socket)
      if self.closing:
        self._socket.close()


class Stream(object):
  def __init__(self, fd, delegate=None):
    # list of ints, each items represents one outstanding call to Stream.read
    self.read_requests = []
    # list of raw data to send to the file
    self.write_buffer = []

    if type(fd) == int:
      self.fd = fd
    else:
      self.fd = os.dup(fd.fileno())

    flags = fcntl.fcntl(self.fd,fcntl.F_GETFL)
    fcntl.fcntl(self.fd, fcntl.F_SETFL, flags | os.O_NDELAY)

    self.delegate = delegate
         
   # TODO: For some reason __del__ is firing before the object is being deleted
   # which in turn is closing the file to soon. It may be do to a deepcopy issue.
   # idea. Log out the ID of this object
   # def __del__(self):
   #   self.close
     
  def __repr__(self):
    return "<stream %s>" % self.fd
     
  def fileno(self):
    return self.fd

  def read(self, bytes):
    self.read_requests.append(bytes)
    current_event_loop().add_reader(self, self.can_read)


         
  def read_to_end(self):
    """Keeps reading and notifying delegate until the end of stream has
    been reached.

    Discussion:
    This function is synchronous and will block the current thread until
    the end of file is reached.
    """

    while 1:
      data = os.read(self.fd, 1024)
      if data:
        self.delegate.onRead(self, data)
      else:
        break
         
  def close(self):
    """Removes the stream from the currentRunLoop and closes the file descriptor"""

    #sys.stderr.write('\033[0;32m')
    #sys.stderr.write('%s %s %s closing %s\n' % (os.getpid(),  threading.currentThread(),sys._getframe(1).f_code.co_name, self.fd))
    #sys.stderr.write('\033[m')


    if self.fd is not None:
     if hasattr(self.delegate, 'on_close'):
       self.delegate.on_close(self)
       # There will be no more data
     del self.read_requests[:]
     
     self.remove_from_event_loop(current_event_loop())
     os.close(self.fd)


     self.fd=None
     
  def can_read(self):
    requestcount = len(self.read_requests)
    while requestcount:
      requestcount -= 1
      # read until we get as much data as we've been
      # waiting for, or the socket would block.
      bytes_2_read = self.read_requests[0]

      try:
        data = os.read(self.fd,bytes_2_read)
        if data == '':
          if hasattr(self.delegate,'end_of_data_for'):
            self.delegate.end_of_data_for(self)
          return                     
      except OSError, e:
        if e.errno != errno.EAGAIN:
          raise

      # notify our delegate that data's been returned

      #wait_if_short = self.delegate.on_read(self, data)
      bytes_read =  len(data)
      if bytes_read < bytes_2_read:# and wait_if_short:
        self.read_requests[0] -= bytes_read
      else:
        # we're done with this request
        del self.read_requests[0]


  def write(self, data):
    self.write_buffer.append(data)
    current_event_loop().add_writer(self.fd, self.can_write)


  def can_write(self):
    bytessent = 0
    while self.write_buffer:
      try:
        data = self.write_buffer[0] 
        # While we have data in our out going buffer try to send it
        sent = os.write(self.fd, data)

        if len(data) == sent:
          # we sent all the data in one shot
          del self.write_buffer[0]
        else:
          self.write_buffer[0] = data[sent:]
        bytessent += sent
      except OSError, e:
        if e.errno == errno.EAGAIN:
          # other end of the socket is full, so
          # wait until we can send more
          # data

          pass
        else:
          raise

    # notify our delegate of how much we wrote in this pass
    if bytessent > 0:
      self.delegate.on_write(self, bytessent)
              
  def on_errror(self, error):
    self.delegate.on_error(self, error)
            
  def remove_from_event_loop(self, event_loop):
    for remove in (event_loop.remove_reader, event_loop.remove_writer):
      try:
        remove(self)
      except KeyError:
        pass
     

from .event_loop import current_event_loop
    