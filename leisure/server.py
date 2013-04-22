from http_parser.pyparser import HttpParser

from .io import puts, indent
from .transports import Socket


import os
import leisure

def start(event_loop):
  socket = Socket(("localhost", 0))
  socket.on("accept", new_connection)
  addr = socket.listen(5, event_loop)
  puts("Accepting connections on {0}:{1}".format(*addr))
  return addr

def new_connection(client):
  parser =HttpParser(kind=0)
  parser.environ = True

  client.on(
    "data", on_read, parser , client
  ).on(
    "error", on_error, client
  )

def on_read(data, parser, client):
  parser.execute(data.tobytes(), len(data))
  if parser.is_headers_complete():
    env = parser.get_wsgi_environ()
    dispatch(env, client)

def on_error(exce, client):
  print exce

def dispatch(env, client):
  sock = client._socket

  out = bytearray()

  def start_response(status, response_headers, exc_info=None):
    out.extend("HTTP/1.1 ")
    out.extend(status)
    out.extend("\r\n")

    for header, value in response_headers:
      out.extend("{}: {}\r\n".format(header, value))

    out.extend("\r\n")

    return sock.send


  headers_sent = False
  sent = 0
  app_iter = app.wsgi_app(env, start_response)


  try:
    for data in app_iter:
      if not headers_sent:
        #puts(out, fore="blue"),

        client.write(out)
        headers_sent = True

      client.write(data)
      sent += len(data)

  finally:
    if hasattr(app_iter, 'close'):
      app_iter.close()

  client.close()


from flask import Flask, request, Response, abort
from .send_file import send_file_partial

app = Flask(__name__)
app.debug = True

@app.after_request
def after_request(response):
  response.headers.add('Accept-Ranges', 'bytes')
  return response


@app.route('/')
def hello_world():
  return "hello"
  

@app.route('/disco/<path:path>')
def disco(path):
  if '..' in path or path.startswith('/'):
    abort(404)

  puts(request.url)
  real_path = os.path.join(os.environ['DISCO_DATA'], path)
  return send_file_partial(real_path)


  



