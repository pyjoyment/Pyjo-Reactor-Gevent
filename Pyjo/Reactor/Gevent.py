"""
Pyjo.Reactor.Gevent - Low-level event reactor on gevent
=======================================================
::

    from Pyjo.Reactor.Gevent import Gevent

    # Watch if handle becomes readable or writable
    reactor = Gevent()
    reactor.io(first, lambda reactor, writable: print(
        'First handle is writable' if writable else 'First handle is readable'
    ))

    # Or import the module, as in Google Python Style Guide

    import Pyjo.Reactor.Gevent

    # Watch if handle becomes readable or writable
    reactor = Pyjo.Reactor.Gevent.new()
    reactor.io(first, lambda reactor, writable: print(
        'First handle is writable' if writable else 'First handle is readable'
    ))

    # Change to watching only if handle becomes writable
    reactor.watch(first, False, True)

    # Watch a file descriptor if it becomes readable
    reactor.io(fd, lambda reactor, writable: print(
        'Second handle is writable' if writable else 'Second handle is readable'
    )).watch(fd, True, False)

    # Add a timer
    def timeout(reactor):
        reactor.remove(first)
        reactor.remove(fd)
        print('Timeout!')

    reactor.timer(15, timeout)

    # Start reactor if necessary
    if not reactor.is_running:
        reactor.start()

:mod:`Pyjo.Reactor.Gevent` is a low-level event reactor based on the hub of
`gevent <https://www.gevent.org>`_, whose loop of libev or libuv watches the
handles and runs the timers, so that the servers of Pyjoyment, as
:mod:`Pyjo.Server.Daemon`, :mod:`Pyjo.Server.Prefork` and
:mod:`Pyjo.Server.Prethread`, and its user agent run along with the greenlets
of a program of gevent. ``PYJO_REACTOR`` selects it for :mod:`Pyjo.IOLoop`::

    PYJO_REACTOR=Pyjo.Reactor.Gevent python myapp.py daemon

The reactor is not implemented yet.

Events
------

:mod:`Pyjo.Reactor.Gevent` inherits all events from :mod:`Pyjo.Reactor.Poll`.

See also
--------

:mod:`Pyjo.Reactor.Poll`, :mod:`Pyjoyment`, https://github.com/pyjoyment/Pyjoyment.

Classes
-------
"""

import Pyjo.Reactor.Poll

import importlib.metadata

from typing import Any


__version__ = importlib.metadata.version('Pyjo-Reactor-Gevent')
"""The version of the distribution, of its metadata, which the workflow Automated version sets in pyproject.toml."""


class Pyjo_Reactor_Gevent(Pyjo.Reactor.Poll.object):
    """
    :mod:`Pyjo.Reactor.Gevent` inherits all methods from
    :mod:`Pyjo.Reactor.Poll`.
    """

    # The template of the distribution: the reactor on the hub of gevent is to be written, so that nothing falls back to
    # the poll of Pyjo.Reactor.Poll in the meantime
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError('Pyjo.Reactor.Gevent is not implemented yet')


new = Pyjo_Reactor_Gevent.new
object = Pyjo_Reactor_Gevent
Gevent = Pyjo_Reactor_Gevent
