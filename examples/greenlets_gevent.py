import os


# The reactor of Pyjoyment on gevent, selected before Pyjo.IOLoop is loaded
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Gevent'

import Pyjo.HelloWorld  # noqa: E402
import Pyjo.IOLoop  # noqa: E402
import Pyjo.Server.Daemon  # noqa: E402
import Pyjo.UserAgent  # noqa: E402

import gevent  # noqa: E402
import gevent.socket  # noqa: E402


# A greenlet with a socket of gevent, which waits in the hub of gevent, where the server of Pyjoyment runs
def with_socket(port):
    with gevent.socket.create_connection(('127.0.0.1', port)) as sock:
        sock.sendall(b'GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n')
        response = b''
        while b'\r\n' not in response and (chunk := sock.recv(4096)):
            response += chunk
    status = response.split(b'\r\n')[0].decode()
    return f'gevent.socket: {status}'


# A greenlet with a blocking request of the user agent of Pyjoyment, whose event loop of its own waits in the greenlet
def with_user_agent(port):
    with Pyjo.UserAgent.new() as ua:
        tx = ua.get(f'http://127.0.0.1:{port}/')
    return f'Pyjo.UserAgent: {tx.result.code} {tx.result.text}'


# The server, on a port that the system picks, which runs in the main greenlet until the greenlets are done, and closes its
# listen sockets at the end of the block
with Pyjo.Server.Daemon.new(app=Pyjo.HelloWorld.new(), listen=['http://127.0.0.1'], silent=True) as daemon:
    port = daemon.start().ports()[0]
    greenlets = [gevent.spawn(with_socket, port), gevent.spawn(with_user_agent, port)]
    gevent.spawn(lambda: (gevent.joinall(greenlets), Pyjo.IOLoop.stop()))
    Pyjo.IOLoop.start()
    for greenlet in greenlets:
        print(greenlet.value)
