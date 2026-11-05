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

The reactor runs in the greenlet that calls :meth:`~Pyjo_Reactor_Gevent.start`
or :meth:`~Pyjo_Reactor_Gevent.one_tick`, the main one or any other, which
waits for the events in the hub, where the other greenlets of the program run
in the meantime, and the callbacks of the reactor run in that greenlet. A
callback can wait for a greenlet or for a function of gevent, as
``gevent.sleep``, which stops the events of the reactor until it returns, but
not the other greenlets. A blocking request of :mod:`Pyjo.UserAgent`, which
runs an event loop of its own, waits in the greenlet that makes it, while the
other greenlets, a server of Pyjoyment among them, run. The reactor cannot run
in the hub itself, as in a callback of the loop of gevent, where gevent raises
``BlockingSwitchOutError``.

A reactor uses the hub of the thread that makes it, so the threads of
:mod:`Pyjo.Server.Prethread` have hubs of their own, and any number of reactors
share the hub of a thread. The child of a fork, as a worker of
:mod:`Pyjo.Server.Prefork`, reinitializes the hub, as ``gevent.os.fork`` does.

The environment variable ``GEVENT_LOOP`` of gevent selects the loop, libev
(``libev-cext``, the default) or libuv (``libuv-cffi``), for which
`cffi <https://pypi.org/project/cffi/>`_ is installed too::

    pip install Pyjo-Reactor-Gevent cffi
    GEVENT_LOOP=libuv-cffi PYJO_REACTOR=Pyjo.Reactor.Gevent python myapp.py daemon

A program that calls ``gevent.monkey.patch_all()`` runs the reactor as well,
but its threads are greenlets then, and so is the thread of
:attr:`Pyjo.IOLoop.Pyjo_IOLoop.singleton`, which each greenlet has of its own,
and :mod:`Pyjo.Server.Prethread` and :mod:`Pyjo.IOLoop.ChildProcess` do not
work with the threads and the subprocesses of gevent.

Events
------

:mod:`Pyjo.Reactor.Gevent` inherits all events from :mod:`Pyjo.Reactor.Poll`.

See also
--------

:mod:`Pyjo.Reactor.Poll`, :mod:`Pyjoyment`, https://github.com/pyjoyment/Pyjoyment.

Classes
-------
"""

import Pyjo.Exception
import Pyjo.Reactor
import Pyjo.Reactor.Poll

from Pyjo.Reactor import Handle, IOCallback, TimerCallback

import importlib.metadata
import os
import threading
import weakref

from collections.abc import Callable
from typing import Any, Self, TypedDict

import gevent
import gevent.hub
import gevent.monkey

from greenlet import getcurrent


__version__ = importlib.metadata.version('Pyjo-Reactor-Gevent')
"""The version of the distribution, of its metadata, which the workflow Automated version sets in pyproject.toml."""

# The events of a watcher of I/O, the same in libev and libuv
_READ = 1
_WRITE = 2

# The thread, not the greenlet that threading takes for it with gevent.monkey
_get_ident: Callable[[], int] = gevent.monkey.get_original('_thread', 'get_ident')


class _Timer(TypedDict):
    cb: TimerCallback | None
    after: float
    recurring: bool
    watcher: Any
    seq: int


class Pyjo_Reactor_Gevent(Pyjo.Reactor.Poll.object):
    """
    :mod:`Pyjo.Reactor.Gevent` inherits all methods from
    :mod:`Pyjo.Reactor.Poll` and implements the following new ones.
    """

    # Not in Mojolicious: gevent, whose hub runs the watchers and the timers of the reactor, and whose code a finalizer of
    # the garbage collector may interrupt
    event_loop_modules = ('gevent',)

    _timers: dict[str, _Timer]  # pyright: ignore[reportIncompatibleVariableOverride]

    hub: Any
    """::

        hub = reactor.hub

    The hub of gevent of the thread that made the reactor, whose loop watches
    the handles and runs the timers.
    """

    def __init__(self) -> None:
        super().__init__()
        # gevent has no types for the hub and its loop
        self.hub = gevent.get_hub()
        loop = self._loop = self.hub.loop
        self._libuv: bool = type(loop).__module__.startswith('gevent.libuv')
        self._ref: weakref.ref[Pyjo_Reactor_Gevent] = weakref.ref(self)
        ref = self._ref

        # The watchers of the handles, which the loop calls in the hub, where the events wait for the greenlet that runs
        # the reactor, and the order of the handles
        self._watchers: dict[int, Any] = {}
        self._ready: dict[int, int] = {}
        self._suspended: set[int] = set()
        self._order: dict[int, int] = {}

        # The timers that are due, which wait for that greenlet too, and the order of the timers
        self._fired: set[str] = set()
        self._seq = 0
        self._timers = {}  # pyright: ignore[reportIncompatibleVariableOverride]

        # The greenlets that wait for the events, and whether they are woken up already
        self._waiting: list[Any] = []
        self._woken = False

        # The watchers of the loop are not referenced, so that the reactor does not keep a program of gevent alive when
        # it does not run; one of these timers is, while a greenlet waits, so that the hub does not end, and wakes it up
        # as the reactor of poll wakes up at least every half a second, or after one iteration of the loop, which polls
        # the handles without waiting, when an event is ready already
        self._keepalive: Any = loop.timer(0.5, 0.5)
        self._poll: Any = loop.timer(0, 0)
        self._keepalive_cb = lambda: _notify(ref)
        weakref.finalize(self, _release, self.hub, self._watchers, self._timers, (self._keepalive, self._poll))

    def again(self, id: str, after: float | None = None) -> None:
        """::

            reactor.again(id)
            reactor.again(id, 0.5)

        Restart timer and optionally change the invocation time. Note that this
        method requires an active timer.
        """
        if (timer := self._timers.get(id)) is None:
            Pyjo.Exception.object.throw('Timer not active')
        if after is not None and after != timer['after']:
            timer['after'] = after
            if (watcher := timer['watcher']) is not None:
                timer['watcher'] = None
                watcher.stop()
                watcher.close()
        self._arm(id, timer)

    def io(self, handle: Handle, cb: IOCallback) -> Self:
        """::

            reactor = reactor.io(handle, cb)

        Watch handle for I/O events, invoking the callback whenever handle
        becomes readable or writable.
        """
        fd = _fileno(handle)
        self._unwatch(fd)
        if fd not in self._io:
            self._seq += 1
            self._order[fd] = self._seq
        self._io[fd] = {'cb': cb, 'mode': 0}
        return self.watch(handle, True, True)

    def one_tick(self) -> None:
        """::

            reactor.one_tick()

        Run reactor until an event occurs or no events are being watched
        anymore, while the other greenlets run.
        """
        running = self._running
        if not running:
            self._running = 1

        try:
            i = 0
            while not i and self._running:
                # The cleanups that finalizers deferred, before the reactor waits
                if Pyjo.Reactor._DEFERRED.get(threading.get_ident()):  # pyright: ignore[reportPrivateUsage]
                    Pyjo.Reactor._drain()  # pyright: ignore[reportPrivateUsage]

                # Stop automatically if there is nothing to watch
                if not len(self._timers) and not len(self._io):
                    return self.stop()

                self._wait(0 if self._ready or self._fired else 0.5)
                i = self._dispatch()
        finally:
            if not running:
                self._running = running

    def recurring(self, after: float, cb: TimerCallback) -> str:
        """::

            id = reactor.recurring(0.25, cb)

        Create a new recurring timer, invoking the callback repeatedly after a
        given amount of time in seconds.
        """
        return self._timer(True, after, cb)

    def remove(self, remove: Handle | str) -> bool:
        """::

            boolean = reactor.remove(handle)
            boolean = reactor.remove(id)

        Remove handle or timer.
        """
        if isinstance(remove, str):
            if (timer := self._timers.pop(remove, None)) is None:
                return False
            self._fired.discard(remove)
            if (watcher := timer['watcher']) is not None:
                watcher.stop()
                watcher.close()
            return True
        fd = _fileno(remove)
        if self._io.pop(fd, None) is None:
            return False
        del self._order[fd]
        self._unwatch(fd)
        return True

    def reset(self) -> None:
        """::

            reactor.reset()

        Remove all handles and timers.
        """
        _stop(self._watchers, self._timers, ())
        self._ready.clear()
        self._suspended.clear()
        self._order.clear()
        self._fired.clear()
        self._events = {}
        self._io = {}
        self._next_tick.clear()
        self._next_timer = None
        self._running = 0

    def stop(self) -> None:
        """::

            reactor.stop()

        Stop watching for I/O and timer events, also from another greenlet
        than the one that runs the reactor.
        """
        self._running = 0
        self._notify()

    def timer(self, after: float, cb: TimerCallback) -> str:
        """::

            id = reactor.timer(0.5, cb)

        Create a new timer, invoking the callback after a given amount of time
        in seconds.
        """
        return self._timer(False, after, cb)

    def watch(self, handle: Handle, read: bool, write: bool) -> Self:
        """::

            reactor = reactor.watch(handle, readable, writable)

        Change I/O events to watch handle for with true and false values. Note
        that this method requires an active I/O watcher.
        """
        fd = _fileno(handle)
        if (io := self._io.get(fd)) is None:
            Pyjo.Exception.object.throw('I/O watcher not active')
        mode = (_READ if read else 0) | (_WRITE if write else 0)
        if mode == io['mode'] and fd not in self._suspended:
            return self
        io['mode'] = mode
        self._suspended.discard(fd)
        if (watcher := self._watchers.get(fd)) is not None:
            watcher.stop()
        if not mode:
            return self
        if watcher is None:
            # libuv sets a handle to the non-blocking mode when it makes the watcher, and never back, where a terminal,
            # which Pyjo.IOLoop.Stdio reads and writes in the blocking mode, shares the mode with the shell
            blocking = self._libuv and os.get_blocking(fd)
            watcher = self._watchers[fd] = self._loop.io(fd, mode, ref=False)
            if blocking:
                os.set_blocking(fd, True)
        else:
            watcher.events = mode
        watcher.start(_io_ready, self._ref, fd, pass_events=True)
        return self

    # A timer of no time is due at once, without a timer of the loop, as the reactor of poll runs the ones that a callback
    # of I/O adds after the I/O of the same tick, where the loop would run them after the I/O of the next one
    def _arm(self, id: str, timer: _Timer) -> None:
        if timer['after'] <= 0:
            if (watcher := timer['watcher']) is not None:
                watcher.stop()
            self._fired.add(id)
            self._notify()
            return
        self._fired.discard(id)
        # The time of the loop is updated, as libev and libuv take the time of the start of the iteration, which the
        # callbacks of the reactor may have taken long after
        if (watcher := timer['watcher']) is None:
            # The interval repeats the timer for again, which stops a timer without one; the callback stops a timer
            # that is not recurring
            watcher = timer['watcher'] = self._loop.timer(timer['after'], timer['after'], ref=False)
            watcher.start(_timer_ready, self._ref, id, update=True)
        else:
            watcher.again(_timer_ready, self._ref, id, update=True)

    # The events that are ready, the handles in the order in which they were added, as the reactor of poll has them,
    # where libev calls the watchers in the reverse order of its backend, then the timers in the order in which they were
    # made
    def _dispatch(self) -> int:
        i = 0
        if self._ready:
            ready, self._ready = self._ready, {}
            order = self._order
            for fd in sorted(ready, key=lambda fd: order.get(fd, 0)) if len(ready) > 1 else ready:
                revents = ready[fd]
                if revents & _READ and (io := self._io.get(fd)) is not None and io['mode'] & _READ:
                    i += 1
                    self._try('I/O watcher', io['cb'], False)
                if revents & _WRITE and (io := self._io.get(fd)) is not None and io['mode'] & _WRITE:
                    i += 1
                    self._try('I/O watcher', io['cb'], True)

        if self._fired:
            fired, self._fired = self._fired, set()
            timers = self._timers
            for id in sorted(fired, key=lambda id: timers[id]['seq'] if id in timers else 0):
                if (t := timers.get(id)) is None:
                    continue

                # Recurring timer of no time, which is due again at the next tick
                if t['recurring']:
                    if not t['after']:
                        self._fired.add(id)

                # Normal timer
                else:
                    self.remove(id)

                if (cb := t['cb']) is not None:
                    i += 1
                    self._try('Timer', cb)
        return i

    # In the hub, from the loop
    def _io_ready(self, fd: int, revents: int) -> None:
        # An error of the handle, which libuv reports with a status, after which it watches the handle no more, while
        # gevent takes it for watched still: readable and writable, as for poll, and watched again at the next wait, so
        # that the callback is called for as long as the error lasts
        error = revents < 0
        if error:
            revents = _READ | _WRITE
        self._ready[fd] = self._ready.get(fd, 0) | revents

        # Nobody waits for the reactor: the handle is not watched until somebody does, as the loop would report it on
        # every iteration, which the other greenlets make
        if (error or not self._waiting) and (watcher := self._watchers.get(fd)) is not None:
            watcher.stop()
            self._suspended.add(fd)
        self._notify()

    # The greenlets that wait are woken up after the events of the iteration of the loop, before it waits again, by a
    # callback of the loop, which gevent runs then
    def _notify(self) -> None:
        if self._waiting and not self._woken:
            self._woken = True
            self._loop.run_callback(_resume, self._ref)

    # In the hub, from the loop; only the greenlets that still wait, as a greenlet that woke up first may have killed
    # another one, which waits for something else then
    def _resume(self) -> None:
        self._woken = False
        for greenlet in list(self._waiting):
            if greenlet in self._waiting:
                greenlet.switch()

    def _timer(self, recurring: bool, after: float, cb: TimerCallback) -> str:
        id = self._id()
        self._seq += 1
        timer: _Timer = {'cb': cb, 'after': after, 'recurring': recurring, 'watcher': None, 'seq': self._seq}
        self._timers[id] = timer
        self._arm(id, timer)
        return id

    # In the hub, from the loop
    def _timer_ready(self, id: str) -> None:
        if (timer := self._timers.get(id)) is None:
            return
        if not timer['recurring'] and (watcher := timer['watcher']) is not None:
            watcher.stop()
        self._fired.add(id)
        self._notify()

    def _unwatch(self, fd: int) -> None:
        self._ready.pop(fd, None)
        self._suspended.discard(fd)
        if (watcher := self._watchers.pop(fd, None)) is not None:
            watcher.stop()
            watcher.close()

    # The greenlet waits in the hub, where the other greenlets run, until the loop has an event of the reactor, or the
    # reactor stops; gevent raises BlockingSwitchOutError in the hub itself
    def _wait(self, timeout: float) -> None:
        current = getcurrent()
        for fd in self._suspended:
            if (watcher := self._watchers.get(fd)) is not None:
                watcher.start(_io_ready, self._ref, fd, pass_events=True)
        self._suspended.clear()

        waiting = self._waiting
        waiting.append(current)
        if len(waiting) == 1:
            self._keepalive.start(self._keepalive_cb, update=True)
        if timeout <= 0 and not self._poll.active:
            self._poll.start(self._keepalive_cb, update=True)
        try:
            self.hub.switch()
        finally:
            waiting.remove(current)
            if not waiting:
                self._keepalive.stop()
                self._poll.stop()

    # The cleanups that finalizers deferred, from any thread
    def _wake(self) -> None:
        self._loop.run_callback_threadsafe(_notify, self._ref)


# The callbacks of the loop refer to the reactor weakly, as the loop of the hub lives as long as the thread
def _io_ready(revents: int, ref: 'weakref.ref[Pyjo_Reactor_Gevent]', fd: int) -> None:
    if (reactor := ref()) is not None:
        reactor._io_ready(fd, revents)  # pyright: ignore[reportPrivateUsage]


def _timer_ready(ref: 'weakref.ref[Pyjo_Reactor_Gevent]', id: str) -> None:
    if (reactor := ref()) is not None:
        reactor._timer_ready(id)  # pyright: ignore[reportPrivateUsage]


def _notify(ref: 'weakref.ref[Pyjo_Reactor_Gevent]') -> None:
    if (reactor := ref()) is not None:
        reactor._notify()  # pyright: ignore[reportPrivateUsage]


def _resume(ref: 'weakref.ref[Pyjo_Reactor_Gevent]') -> None:
    if (reactor := ref()) is not None:
        reactor._resume()  # pyright: ignore[reportPrivateUsage]


# The child of a fork reinitializes the hub, which gevent.os.fork does, as os.fork of gevent.monkey
def _after_fork_in_child() -> None:
    if not gevent.monkey.is_module_patched('os'):
        gevent.hub.reinit()


def _fileno(handle: Handle) -> int:
    fd = handle if isinstance(handle, int) else handle.fileno()
    if fd < 0:
        Pyjo.Exception.object.throw('Handle is closed')
    return fd


# The watchers that an object leaves in the loop when it is freed, in the thread of the hub, as the garbage collector runs
# a finalizer in any thread
def _release(hub: Any, watchers: dict[int, Any], timers: dict[str, _Timer], aux: tuple[Any, ...]) -> None:
    if hub.thread_ident == _get_ident():
        _stop(watchers, timers, aux)
    else:
        hub.loop.run_callback_threadsafe(_stop, watchers, timers, aux)


def _stop(watchers: dict[int, Any], timers: dict[str, _Timer], aux: tuple[Any, ...]) -> None:
    for watcher in watchers.values():
        watcher.stop()
        watcher.close()
    watchers.clear()
    for timer in timers.values():
        if (watcher := timer['watcher']) is not None:
            watcher.stop()
            watcher.close()
    timers.clear()
    for watcher in aux:
        watcher.stop()
        watcher.close()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_after_fork_in_child)

new = Pyjo_Reactor_Gevent.new
object = Pyjo_Reactor_Gevent
Gevent = Pyjo_Reactor_Gevent
