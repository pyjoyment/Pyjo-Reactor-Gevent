import os


# The template of the test of the reactor, to be t/Pyjo/Reactor_Poll.py of Pyjoyment, the port of t/mojo/reactor_poll.t
# of Mojolicious, with subtests of its own for the hub of gevent. The reactor of the test is selected before Pyjo.IOLoop
# is loaded, as Pyjo.Reactor.detect() prefers Pyjo.Reactor.Asyncio
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Gevent'

import Pyjo.Reactor  # noqa: E402
import Pyjo.Reactor.Gevent  # noqa: E402

from Pyjo.Test import *  # noqa: E402


with subtest('Reactor detection'):
    is_ok(Pyjo.Reactor.object.detect(), 'Pyjo.Reactor.Gevent', 'right class')
    reactor = Pyjo.Reactor.Gevent.new()
    is_ok(type(reactor), Pyjo.Reactor.Gevent.object, 'right object')

done_testing()
