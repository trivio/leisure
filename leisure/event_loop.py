import os
import heapq
import time
import threading
import select

local = threading.local()
import logging


def stop():
  """Stops the current event loop"""
  current_event_loop().stop()

def call_soon(method, *args):
  return Future()

def current_event_loop():
  if not hasattr(local, 'event_loop'):
    local.event_loop = EventLoop()
  return local.event_loop

def add_reader(fd, callback, *args):
  current_event_loop().add_reader(fd, callback, *args)

def remove_reader(fd):
  current_event_loop().remove_reader(fd)


def fileno(fd):
  if isinstance(fd, int):
    return fd
  else:
    return fd.fileno()


class EventLoop(object):
  """An event loop that provides edge-triggered notifications.

  This EventLoop monitors a series of file descriptors, timers and
  OS signals for events.

  Each pass through the EventLoop we check to see if any timer has
  expired at which point we call the timer's timeout() method
  giving the Timer an oprotunity to preform any neccary actions.

  After notifying each expired Timer we calculate how long until
  the next timer (if any) will expire.

  We then ask the OS to put us to sleep until one or more of
  our file descriptors has data ready to be read or written to; or
  our timeout has expired.

  When we wake up it's because  one of our descriptors
  are in the ready state, a timer has expired or both.

  If one of our descriptors is ready we remove it from the list of
  descriptors to be monitored and then notify the apportiate
  callback/delegate that it can now read or write the descriptor
  without blocking. Note: it's the responsabilty of the delegate
  to ask the EventLoop to remonitor a descriptor

  And that's it the loop starts over if there are any timers or
  descriptors left to be monitored.

  You do not need to instatiate a EventLoop, there should only be
  one per thread. To get the EventLoop for the current thread simply
  call the class method currentLoop()

  >>> EventLoop = EventLoop.currentEventLoop()

  To determine if the EventLoop is running you can examine it's
  running property, in this paticular case we're not running

  >>> EventLoop.running 
  False

  To start the EventLoop you must call run(), this will block the
  thread until the EventLoop runs out of things to montior. Since we
  have nothing to montior calling run() will return right away.
  >>> EventLoop.run()
  >>> EventLoop.running
  False

  That's pretty boring, let's create a class that support's the
  Timer interface and have our object called imideiatly. Timer's
  need two attributes a timeout value, which is the in seconds (as
  typically returned by time.time()) after which the timer's
  timeout() method should be called.

  >>> class MyTimer:
  ...   def __init__(self):
  ...     self.time = time.time()
  ...     self.cancelled = self.called = False
  ...     self.timeOutCalled = False
  ...   def onTimeout(self):
  ...     self.timeWhenCalled = time.time()
  ...     self.timeOutCalled = True

  >>> myTimer = MyTimer()

  So we have a Timer, it has an attribute called timeOutCalled
  which is currently false

  >>> myTimer.timeOutCalled
  False

  We add it to the EventLoop then run the EventLoop
  >>> timer = EventLoop.addTimer(myTimer)
  >>> EventLoop.run()

  And when the EventLoop completes our timer's timeout value should
  have been called.
  >>> myTimer.timeOutCalled
  True

  Noticed that the code returned imediatly because after signaling
  this timer there was nothing else to monitor. Typically
  applications that use a EventLoop will always ensure that there's
  something to monitor. For instance we can make a component that
  get's called once every millisecond for 10 miliseconds by simply
  readding the Timer back to the EventLoop in the Timer's timeout
  method like this.

  >>> class HeartBeat:
  ...   def __init__(self):
  ...     self.time = time.time() + .01
  ...     self.cancelled = self.called = False
  ...     self.ticks = 0
  ...   def onTimeout(self):
  ...     self.ticks += 1
  ...     if self.ticks < 10:
  ...       self.time = time.time() + .01
  ...       EventLoop.currentEventLoop().addTimer(self)

  Notice in this example a couple of things, for one we set
  HeartBeat.time to be the current time plus ".01". In other words
  we want are timeout() method to be called 1 milisecond from
  now. We keep track of how many times we're called, if it's less
  than 10 we reschedule ourselves back in the current
  EventLoop. This demonstrates how an object doesn't need to keep a
  reference to the EventLoop to use it.

  >>> timer = HeartBeat()
  >>> timer.ticks
  0

  >>> timer = EventLoop.addTimer(timer)
  >>> EventLoop.run()
  >>> timer.ticks
  10

  Normally you wouldn't implement your own Timer class because
  most of the basic ones that you'd need have already been
  implemented for you like a DeferedCall which will run a specific
  method after a certain delay and optionally repeat if neccesary.


  """


  def __init__(self):
    self.threadCallQueue = []
    self.readers = {}
    self.writers = {}

    reader, writer = os.pipe()
    self.waker = writer
    
    self.waker_reader = Stream(reader)
    self.add_reader(self.waker_reader, self.on_wakeup)


    self.running = False
    self.timers = []

  @property
  def log(self):
    return logging      

  def add_reader(self, fd, callback, *args): 
    self.readers[fileno(fd)] = (callback, args)

  def remove_reader(self, fd):
    del self.readers[fileno(fd)]

  def add_writer(self, fd, callback, *args):
    self.writers[fileno(fd)] = (callback, args)

  def remove_writer(self, fd):
    del self.writers[fileno(fd)]
        
  def reset(self):
    self.running = False
    self.readers = {}
    self.writers = {}
    self.timers  = []
    self.threadCallQueue = []
    self.waker_reader.read(1)



  def _shouldRun(self,timerCapacity):
     # Internal method, determines if the EventLoop should be stooped.

     # EventLoop.run() will call this method with a value of 0,
     # indicating that if there are any timers, then the EventLoop
     # should continue until they fire

     # EventLoop.runUntil() will call this method with a value of
     # one, indicating that there must be more than 1 timer, or
     # else the EventLoop should quit. This is because runUntil()
     # adds one timer to stop the EventLoop at the specified time,
     # but this timer shouldn't be considered something that keeps
     # the EventLoop going if there is no other activity to monitor.


     # Keep calling the EventLoop until some one stops us, we
     # have no timers or the readers and writers drops to 1
     # (EventLoops keep one reader around to wake
     # themselves up from a sleep)
     return self.running and (len(self.readers) + len(self.writers)  > 1 or
                              len(self.timers) > timerCapacity or self.threadCallQueue)


  def quitOnExceptionHandler(self, exception):
    self.log.exception("Caught unexpected error in RunOnce.")
    self.stop()
  handleException = quitOnExceptionHandler  
    

  def run(self, reset_on_stop=True):
    """Keeps the EventLoop going until it's explicitly stoped or it runs out
    of things to monitor."""

    if self.running:
      raise RuntimeError("EventLoop is already running.")
    else:
      self.running = True
      
    while self.running: #self._shouldRun(0):
      try:
        self.runOnce()
      except Exception, e:
        self.handleException(e)

    if reset_on_stop:      
      self.reset()


  def runUntil(self, stopDate=None, **kw):
     """Runs the EventLoop until the given time plus interval have been
     reached or it runs out of things to monitor. This method
     should not be called when the EventLoop is already running.

     The current time is assumed, if no date time is passed in.

     Examples:(note, these aren't real doctests yet)

     Run until a given date, say St. Patty's day
     >> date=datetime.datetime(2007, 03,17, 17,00)
     >> EventLoop.currentEventLoop().runUntil(dateAndTime)

     Additionally you can pass in any keyword argument normally
     taken by daetutilse.relativedelta to derive the date. These
     include:

     years, months, weeks, days, hours, minutes, seconds, microseconds

     These are moste useful when you want to compute the relative
     offset from now. For example to run the EventLoop for 5 seconds
     you could do this.

     >> EventLoop.currentEventLoop().runUntil(seconds=5)

     Or, probably not as practical but still possible, wait one
     year and 3 days

     >> EventLoop.currentEventLoop().runUntil(years=1, days=3)
     

     """

     if self.running:
        raise RuntimeError("EventLoop is already running.")
     else:
        self.running = True

     delta = relativedelta(**kw)
     now = datetime.datetime.now()
     
     if stopDate is None:
        stopDate = now

     stopDate = now + delta

     # convert the time back into seconds since the epoch,
     # subtract now from it, and this will then be the delay we
     # can use

     seconds2Run = time.mktime(stopDate.timetuple()) - time.mktime(now.timetuple())
     self.waitBeforeCalling(seconds2Run, self.stop)
     
     while self._shouldRun(1):
        try:
           self.runOnce()
        except:
           self.log.exception("Caught unexpected error in RunOnce.")
           
     self.reset()

        

  def runOnce(self):

     # call every fucnction that was queued via callFromThread up
     # until this point, but nothing more. If not we could be
     # stuck doing this forever and never getting to the other calls
     
     pending = len(self.threadCallQueue)
     tried   = 0
     try:
        for (f, a, kw) in self.threadCallQueue[:pending]:
           tried += 1
           f(*a, **kw)
           
     finally:
        # it's possible that more calls could have came in since we
        # started, bu they should be on the end of the list
        del self.threadCallQueue[:tried]


     # we sleep until we either receive data or our earliest
     # timer has expired.


     currentTime = time.time()
     # fire every timer that's expired

     while self.timers:
           timer   = heapq.heappop(self.timers)
           if timer.cancelled:
                 continue

           timeout = timer.time - currentTime
           if timeout <= 0:
                 # it's expired call it
                 timer.onTimeout()
           else:
                 # this timer hasn't expired put it back on the list
                 heapq.heappush(self.timers, timer)
                 break

     else:
           if (len(self.readers) + len(self.writers)) < 1:
                 # we don't have any timers, if we're not monitoring
                 # any descriptors we need to bail
                 return
           else:
                 # no timed events but we have file descriptors
                 # to monitor so sleep until they have
                 # activity.

                 timeout = None 

     try:
           ready2Read, ready2Write, hadErrors =\
                       select.select(self.readers.keys(), 
                                     self.writers.keys(), 
                                     [], timeout)
     except (select.error, IOError), e:
           if e.args[0] == errno.EINTR:

                 # a signal interupted our select, hopefully
                 # someone eles is handling signals and using
                 # callFromeThread to do the right thing.
                 return
           elif e.args[0] == errno.EBADF:
             # ugh
             self.clear_bad_descriptor()
             return
           else:
                 raise


     while ready2Read or ready2Write or hadErrors:
           # note the popping alows us not get hung up doing all reads all writes
           # at once, not sure how useful this is.
           if ready2Read:
                 fileno = ready2Read.pop()
                 callback, args = self.readers[fileno]#.pop(fileno)
                 callback(*args)
                 #stream.canRead(stream)
                 #stream.handleEvent(stream,Stream.HAS_BYTES_AVAILABLE)

           if ready2Write:
                 writer = ready2Write.pop()
                 # writers, when ready will always be ready. To
                 # avoid an infinite loop an app that wishes to
                 # read the data they must call addWriter()
                 # again
                 stream = self.writers.pop(writer, None)
                 # stream will be none if a method called during ready2read removed
                 # it prior to checking the writers.
                 if stream: 
                   stream.canWrite(stream)
                    #stream.handleEvent(stream, Stream.HAS_SPACE_AVAILABLE)
  def stop(self):
    # drop us out of the run loop on it's next pass
    self.running = False 
    self.wakeup()

  def addTimer(self, timer):
    heapq.heappush(self.timers, timer)
    self.wakeup()
    # we return the timer for convienance sake
    return timer

  def wakeup(self):
    os.write(self.waker, 'x') # write one byte to wake up the EventLoop

  def on_wakeup(self):
    # we've been woken up, ignore the data and readAgain which
    # should schedule us once more back in the EventLoop
    self.waker_reader.read(1)


  def call_later(self, seconds, method, *args,  **kw):
    # Create a non repeating event
    dc = DelayedCall(seconds, method, *args,  **kw)
    self.addTimer(dc)
    return dc

  def call_soon(self, callback, *args):
    return self.call_later(0, callback, *args)

  def call_soon_threadsafe(self, f, *args):
    assert callable(f), "%s is not callable" % f
    self.threadCallQueue.append((f, args, kw))
    self.wakeup()


  def intervalBetweenCalling(self, secondsOrRRule, method, *args, **kw):
     # Create a repeating event, this method can be called
     # either with the number of seconds between each call or
     # it can be passed a string or dateutil.rrule

     t = type(secondsOrRRule)
     # Convert to an RRULe if it's a string or a number
     if t in (int, float):
        rule = rrule.rrule(rrule.SECONDLY, interval=secondsOrRRule)
     elif isinstance(secondsOrRRule, basestring):
        rule = rrule.rrulestr(secondsOrRRule)
     else:
        # hopefully it's an object that returns an iteration of datetime objects
        rule = secondsOrRRule
        
     dc = DelayedCall(iter(rule), method, *args,  **kw)
     self.addTimer(dc)
     return dc
     
  def clear_bad_descriptor(self):
    # ugh not pretty when this happens
    
    for key in self.readers.keys():
      try:
        select.select([key],[],[], 0)
      except Exception, e:
        bad = self.readers.pop(key)
        bad.onError(e)

    for key in self.writers.keys():
      try:
        select.select([],[key],[], 0)
      except Exception, e:
        bad = self.writers.pop(key)
        bad.onError(e)
    
class DelayedCall:
  def __init__(self, secondsOrRRule, func, *args, **kw):
     self.repeatRule = None
     try:
        self.time = time.time() + secondsOrRRule
     except TypeError:
        # it's not a number of seconds hopefully it's an rrule that's been converted to a generator
        self.repeatRule = secondsOrRRule
        dt = self.repeatRule.next()
        self.time = time.mktime(dt.timetuple()) #time.time() + self.delay
     
     self.cancelled = self.called = False
     self.func = func
     self.args = args
     self.kw = kw
     self.delayed_time = 0

  def __cmp__(self, other):
     return cmp(self.time, other.time)

  def onTimeout(self):  
    if not self.cancelled:
      self.func(*self.args, **self.kw)
      if self.repeatRule:
        try:
          dt = self.repeatRule.next()
          self.time = time.mktime(dt.timetuple()) #time.time() + self.delay
          current_event_loop().addTimer(self)
        except StopIteration: # rule has been exhausted
          pass

  def cancel(self):
    """Unschedule this call

    @raise AlreadyCancelled: Raised if this call has already been
    unscheduled.

    @raise AlreadyCalled: Raised if this call has already been made.
    """
    if self.cancelled:
       raise RuntimeError("Already Cancelled")
    elif self.called:
       raise RuntimeError("Already Called")
    else:
       self.cancelled = True
       del self.func, self.args, self.kw



class Future(object):
  counter = 0
  def __init__(self):
    Future.counter += 1
    self.id = Future.counter

from .transports import Stream