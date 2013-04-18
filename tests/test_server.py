import sys
import os
from unittest import TestCase
from nose.tools import eq_
import shutil
import tempfile
import threading

import requests

from leisure import server, event_loop
import logging
logging.basicConfig(stream=sys.stderr)

from StringIO import StringIO
import gzip


class TestServer(object):
  def setUp(self):
    self.data_root = tempfile.mkdtemp()
    os.environ['DISCO_DATA'] = self.data_root
    self.event_loop = event_loop
    self.event_loop.call_later(2, lambda: event_loop.stop())

  def tearDown(self):
    shutil.rmtree(self.data_root)

  def request(self, method, path=''):
    loop = event_loop.current_event_loop()
    context = []
    def fetch_data(addr):
      def _(): # requests is blocking so needs it's own thread

        url = "http://{1}:{2}/{0}".format(path, *addr)
        #import pdb; pdb.set_trace()
        context.append( requests.get(url, timeout=3))
        loop.stop()
      t = threading.Thread(target=_)
      t.daemon = True
      t.start()

    addr = server.start(event_loop)
    self.event_loop.call_soon(fetch_data, addr)
    self.event_loop.run()
    return context.pop()

  def get(self, path=''):
    return self.request('GET', path)


  def test_get_compressed(self):
    content = "line 1\nline 2\n" * 1024**2
    index = gzip.GzipFile(os.path.join(self.data_root, 'index.gz'), 'w')
    index.write(content)
    index.close()

    resp = self.get("disco/index.gz")

    data = gzip.GzipFile(fileobj=StringIO(resp.content)).read()

    eq_(data, content)


