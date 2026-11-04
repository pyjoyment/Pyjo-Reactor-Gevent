import os


# The reactor of Pyjoyment on gevent, selected before Pyjo.IOLoop is loaded
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Gevent'

import Pyjo.HelloWorld  # noqa: E402
import Pyjo.IOLoop  # noqa: E402
import Pyjo.Server.Daemon  # noqa: E402
import Pyjo.UserAgent  # noqa: E402

import gevent  # noqa: E402


# The server, on a port that the system picks, and the user agent, in the same event loop, which close their sockets at
# the end of the block
with (
    Pyjo.Server.Daemon.new(app=Pyjo.HelloWorld.new(), listen=['http://127.0.0.1'], silent=True) as daemon,
    Pyjo.UserAgent.new(ioloop=Pyjo.IOLoop.singleton) as ua,
):
    # A blocking request, which runs the event loop of the server on the hub of gevent until the response
    tx = ua.get(f'http://127.0.0.1:{daemon.start().ports()[0]}/')
    reactor = Pyjo.IOLoop.singleton.reactor
    print(type(reactor).__name__, 'gevent' if getattr(reactor, 'hub', None) is gevent.get_hub() else 'other')
    print(tx.result.code, tx.result.text)
