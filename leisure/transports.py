import fcntl
import os

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
      wait_if_short = self.delegate.on_read(self, data)
      bytes_read =  len(data)
      if bytesRead < bytes_2_read and wait_if_short:
        self.read_requests[0] -= bytesRead
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
    