from collections import defaultdict
class EventEmmiter(object):

  def on(self, event, callback, *args):
    if not hasattr(self, "_callbacks"):
      self._callbacks = defaultdict(list)

    self._callbacks[event].append((callback, args))
    return self

  def fire(self, event, sender):
    for callback, args in self._callbacks[event]:
      callback(sender, *args)

 