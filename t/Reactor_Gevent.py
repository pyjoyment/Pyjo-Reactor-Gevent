import os


# Unlike Mojolicious: the reactor of the test is selected before Pyjo.IOLoop is loaded, as Mojo::Reactor->detect prefers
# Mojo::Reactor::EV where it is installed, and Pyjo.Reactor.detect() prefers Pyjo.Reactor.Asyncio
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Gevent'

import Pyjo.Reactor  # noqa: E402
import Pyjo.Reactor.Gevent  # noqa: E402
import Pyjo.Reactor.Poll  # noqa: E402
import Pyjo.Server.Daemon  # noqa: E402
import Pyjo.UserAgent  # noqa: E402
import Pyjoyment  # noqa: E402

from Pyjo.Loader import load_class  # noqa: E402
from Pyjo.Test import *  # noqa: E402

import signal  # noqa: E402
import socket  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402
import weakref  # noqa: E402

import gevent  # noqa: E402
import gevent.exceptions  # noqa: E402


def eval_(cb):
    try:
        cb()
    except Exception as e:
        return e


# Shared between subtests, since I/O watchers and timers keep firing across them
readable = writable = 0


def rw_cb(reactor, is_writable):
    global readable, writable
    if is_writable:
        writable += 1
    else:
        readable += 1


with subtest('Instantiation'):
    reactor = Pyjo.Reactor.Gevent.new()
    is_ok(type(reactor), Pyjo.Reactor.Gevent.object, 'right object')
    is_ok(type(Pyjo.Reactor.Gevent.new()), Pyjo.Reactor.Gevent.object, 'right object')
    del reactor
    is_ok(type(Pyjo.Reactor.Gevent.new()), Pyjo.Reactor.Gevent.object, 'right object')
    # Not in Mojolicious: the constructor of Python, as in Pyjoyment, and the reactor of poll, which it extends
    is_ok(Pyjo.Reactor.Gevent.Gevent, Pyjo.Reactor.Gevent.object, 'right class')
    is_ok(type(Pyjo.Reactor.Gevent.Gevent()), Pyjo.Reactor.Gevent.object, 'right object')
    ok(issubclass(Pyjo.Reactor.Gevent.object, Pyjo.Reactor.Poll.object), 'reactor of poll')
    ok(not load_class('Pyjo.IOLoop'), 'use Pyjo.IOLoop')
    reactor = Pyjo.IOLoop.singleton.reactor
    is_ok(type(reactor), Pyjo.Reactor.Gevent.object, 'right object')

with subtest('Make sure it stops automatically when not watching for events'):
    triggered = 0

    def triggered_cb(loop):
        global triggered
        triggered += 1

    Pyjo.IOLoop.next_tick(triggered_cb)
    Pyjo.IOLoop.start()
    ok(triggered, 'reactor waited for one event')
    started = time.time()
    Pyjo.IOLoop.start()
    Pyjo.IOLoop.one_tick()
    ok(time.time() < (started + 10), 'stopped automatically')

with subtest('Listen'):
    listen = socket.create_server(('127.0.0.1', 0), backlog=5)
    port = listen.getsockname()[1]
    reactor.io(listen, rw_cb).watch(listen, False, False).watch(listen, True, True)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(not readable, 'handle is not readable')
    ok(not writable, 'handle is not writable')

with subtest('Connect'):
    client = socket.create_connection(('127.0.0.1', port))
    reactor.timer(1, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable')
    ok(not writable, 'handle is not writable')

with subtest('Accept'):
    server = listen.accept()[0]
    ok(reactor.remove(listen), 'removed')
    ok(not reactor.remove(listen), 'not removed again')
    readable = writable = 0
    reactor.io(client, rw_cb)
    reactor.again(reactor.timer(0.025, lambda reactor: reactor.stop()))
    reactor.start()
    ok(not readable, 'handle is not readable')
    ok(writable, 'handle is writable')
    client.sendall(b'hello!\n')
    time.sleep(1)
    ok(reactor.remove(client), 'removed')
    readable = writable = 0
    reactor.io(server, rw_cb)
    reactor.watch(server, True, False)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable')
    ok(not writable, 'handle is not writable')
    readable = writable = 0
    reactor.watch(server, True, True)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable')
    ok(writable, 'handle is writable')
    readable = writable = 0
    reactor.watch(server, False, False)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(not readable, 'handle is not readable')
    ok(not writable, 'handle is not writable')
    readable = writable = 0
    reactor.watch(server, True, False)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable')
    ok(not writable, 'handle is not writable')
    readable = writable = 0
    reactor.io(server, rw_cb)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable')
    ok(writable, 'handle is writable')

with subtest('Timers'):
    timer = recurring = 0

    def timer_cb(reactor):
        global timer
        timer += 1

    def recurring_cb(reactor):
        global recurring
        recurring += 1

    reactor.timer(0, timer_cb)
    ok(reactor.remove(reactor.timer(0, timer_cb)), 'removed')
    id = reactor.recurring(0, recurring_cb)
    readable = writable = 0
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable again')
    ok(writable, 'handle is writable again')
    ok(timer, 'timer was triggered')
    ok(recurring, 'recurring was triggered')
    done = False
    readable = writable = timer = recurring = 0

    def done_cb(reactor):
        global done
        done = reactor.is_running

    reactor.timer(0.025, done_cb)
    while not done:
        reactor.one_tick()
    ok(readable, 'handle is readable again')
    ok(writable, 'handle is writable again')
    ok(not timer, 'timer was not triggered')
    ok(recurring, 'recurring was triggered again')
    readable = writable = timer = recurring = 0
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable again')
    ok(writable, 'handle is writable again')
    ok(not timer, 'timer was not triggered')
    ok(recurring, 'recurring was triggered again')
    ok(reactor.remove(id), 'removed')
    ok(not reactor.remove(id), 'not removed again')
    readable = writable = timer = recurring = 0
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable again')
    ok(writable, 'handle is writable again')
    ok(not timer, 'timer was not triggered')
    ok(not recurring, 'recurring was not triggered again')
    readable = writable = timer = recurring = 0
    next_tick = 0

    def next_tick_cb(reactor):
        global next_tick
        next_tick += 1

    is_ok(reactor.next_tick(next_tick_cb), None, 'returned None')
    id = reactor.recurring(0, recurring_cb)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable again')
    ok(writable, 'handle is writable again')
    ok(not timer, 'timer was not triggered')
    ok(recurring, 'recurring was triggered again')
    ok(next_tick, 'next tick was triggered')

with subtest('Reset'):

    def reset_failed_cb(reactor):
        raise Exception('Reset failed')

    reactor.next_tick(reset_failed_cb)
    reactor.reset()
    readable = writable = recurring = 0
    reactor.next_tick(lambda reactor: reactor.stop())
    reactor.start()
    ok(not readable, 'io event was not triggered again')
    ok(not writable, 'io event was not triggered again')
    ok(not recurring, 'recurring was not triggered again')
    reactor2 = Pyjo.Reactor.Gevent.new()
    is_ok(type(reactor2), Pyjo.Reactor.Gevent.object, 'right object')

with subtest('Ordered next_tick'):
    result = []
    for i in range(1, 11):
        reactor.next_tick(lambda reactor, i=i: result.append(i))
    reactor.start()
    is_deeply_ok(result, list(range(1, 11)), 'right result')

with subtest('Reset while watchers are active'):
    writable = 0

    def reset_cb(reactor, is_writable):
        global writable
        writable += 1
        reactor.reset()

    for handle in (client, server):
        reactor.io(handle, reset_cb).watch(handle, False, True)
    reactor.start()
    is_ok(writable, 1, 'only one handle was writable')
    client.close()
    server.close()

with subtest('Concurrent reactors'):
    timer = 0
    reactor.recurring(0, timer_cb)
    timer2 = 0

    def timer2_cb(reactor):
        global timer2
        timer2 += 1

    reactor2.recurring(0, timer2_cb)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(timer, 'timer was triggered')
    ok(not timer2, 'timer was not triggered')
    timer = timer2 = 0
    reactor2.timer(0.025, lambda reactor: reactor.stop())
    reactor2.start()
    ok(not timer, 'timer was not triggered')
    ok(timer2, 'timer was triggered')
    timer = timer2 = 0
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(timer, 'timer was triggered')
    ok(not timer2, 'timer was not triggered')
    timer = timer2 = 0
    reactor2.timer(0.025, lambda reactor: reactor.stop())
    reactor2.start()
    ok(not timer, 'timer was not triggered')
    ok(timer2, 'timer was triggered')
    reactor.reset()

with subtest('Restart timer'):
    single = pair = last = 0

    def single_cb(reactor):
        global single
        single += 1

    def one_cb(reactor):
        global pair, last
        if single and pair:
            last += 1
        pair += 1
        reactor.stop() if pair > 1 else reactor.again(two)

    def two_cb(reactor):
        global pair, last
        if single and pair:
            last += 1
        pair += 1
        reactor.stop() if pair > 1 else reactor.again(one)

    reactor.timer(0.025, single_cb)
    one = reactor.timer(0.025, one_cb)
    two = reactor.timer(0.025, two_cb)
    reactor.start()
    is_ok(pair, 2, 'timer pair was triggered')
    ok(single, 'single timer was triggered')
    ok(last, 'timers were triggered in the right order')

with subtest('Reset timer'):
    before = time.monotonic()
    after = None
    again = 0

    def after_cb(reactor):
        global after
        after = time.monotonic()

    def again_cb(reactor):
        global again
        again += 1
        if again > 3:
            reactor.remove(two)

    def restart_cb(reactor):
        reactor.again(one, 0.025)
        reactor.again(two, 0.025)

    one = reactor.timer(300, after_cb)
    two = reactor.recurring(300, again_cb)
    reactor.timer(0.025, restart_cb)
    reactor.start()
    ok(after, 'timer was triggered')
    ok((after - before) < 200, 'less than 200 seconds')
    is_ok(again, 4, 'recurring timer triggered four times')

with subtest('Restart inactive timer'):
    id = reactor.timer(0, lambda reactor: None)
    ok(reactor.remove(id), 'removed')
    err = eval_(lambda: reactor.again(id))
    like_ok(str(err), r'Timer not active', 'right error')

with subtest('Change inactive I/O watcher'):
    ok(not reactor.remove(listen), 'not removed again')
    err = eval_(lambda: reactor.watch(listen, True, True))
    like_ok(str(err), r'I/O watcher not active', 'right error')
    listen.close()

with subtest('Error'):
    err = None

    def error_cb(reactor, e):
        global err
        reactor.stop()
        err = e

    def die_cb(reactor):
        raise Exception('works!\n')

    reactor.unsubscribe('error').on('error', error_cb)
    reactor.timer(0, die_cb)
    reactor.start()
    like_ok(str(err), r'works!', 'right error')

    # Not in Mojolicious: the arguments of a callback, which Perl does not check, as in Pyjo.Reactor.Poll
    reactor.timer(0, lambda: None)
    reactor.start()
    like_ok(
        str(err),
        r'takes 0 positional arguments but 1 was given: the callbacks of a reactor get the reactor first',
        'hint',
    )

with subtest('Reset events'):
    reactor.on('error', lambda reactor, err: None)
    ok(reactor.has_subscribers('error'), 'has subscribers')
    reactor.reset()
    ok(not reactor.has_subscribers('error'), 'no subscribers')

with subtest('Recursion'):
    timer = 0

    def recursion_cb(reactor):
        global timer
        timer += 1
        reactor.one_tick()

    reactor = Pyjo.Reactor.Gevent.new()
    reactor.timer(0, recursion_cb)
    reactor.one_tick()
    is_ok(timer, 1, 'timer was triggered once')

with subtest('Detection'):
    is_ok(Pyjo.Reactor.object.detect(), 'Pyjo.Reactor.Gevent', 'right class')

with subtest('Reactor in control'):
    is_ok(type(Pyjo.IOLoop.singleton.reactor), Pyjo.Reactor.Gevent.object, 'right object')
    ok(not Pyjo.IOLoop.is_running(), 'loop is not running')
    buffer = b''
    server_err = server_running = client_err = client_running = None

    def server_cb(loop, stream, cid):
        global server_err, server_running
        stream.write(b'test', lambda stream: stream.write(b'321'))
        server_running = Pyjo.IOLoop.is_running()
        server_err = eval_(lambda: Pyjo.IOLoop.start())

    id = Pyjo.IOLoop.server(server_cb, address='127.0.0.1')
    port = Pyjo.IOLoop.acceptor(id).port

    def client_cb(loop, err, stream):
        global client_err, client_running

        def read_cb(stream, chunk):
            global buffer
            buffer += chunk
            if buffer != b'test321':
                return
            Pyjo.IOLoop.singleton.reactor.stop()

        stream.on('read', read_cb)
        client_running = Pyjo.IOLoop.is_running()
        client_err = eval_(lambda: Pyjo.IOLoop.start())

    Pyjo.IOLoop.client(client_cb, port=port)
    Pyjo.IOLoop.singleton.reactor.start()
    ok(not Pyjo.IOLoop.is_running(), 'loop is not running')
    like_ok(str(server_err), r'^Pyjo.IOLoop already running', 'right error')
    like_ok(str(client_err), r'^Pyjo.IOLoop already running', 'right error')
    ok(server_running, 'loop is running')
    ok(client_running, 'loop is running')

# Left out from Mojolicious: the abstract methods of Mojo::Reactor, which t/Pyjo/Reactor_Poll.py of Pyjoyment tests

# Not in Mojolicious: a callback of next_tick from a signal handler, which Python runs in the middle of what the reactor
# does, as the servers stop gracefully, with enough timers that the handler interrupts the reactor that looks for the
# first of them, as in Pyjo.Reactor.Poll
with subtest('next_tick from a signal handler'):
    if not hasattr(signal, 'setitimer'):
        plan(skip_all='signal.setitimer is required')
    reactor = Pyjo.Reactor.Gevent.new()
    for _ in range(100000):
        reactor.timer(10, lambda reactor: None)
    ticks = []
    handler = signal.signal(signal.SIGALRM, lambda signum, frame: reactor.next_tick(lambda reactor: ticks.append(1)))
    signal.setitimer(signal.ITIMER_REAL, 0.001, 0.001)
    reactor.timer(0.3, lambda reactor: reactor.stop())
    try:
        err = eval_(lambda: reactor.start())
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, handler)
    is_ok(err, None, 'no error')
    ok(ticks, 'callbacks of the signal handler')
    reactor.reset()


# Not in Mojolicious: the hub of gevent, which is what the reactor adds to Pyjo.Reactor.Poll; any number of reactors on
# the hub of the thread that makes them
with subtest('Hub of the thread'):
    reactor = Pyjo.Reactor.Gevent.new()
    ok(reactor.hub is gevent.get_hub(), 'hub of the main thread')
    ok(Pyjo.Reactor.Gevent.new().hub is reactor.hub, 'shared by another reactor')
    result = []

    def thread():
        reactor = Pyjo.Reactor.Gevent.new()
        reactor.timer(0.01, lambda reactor: result.append(reactor.hub is gevent.get_hub()))
        reactor.start()
        result.append(reactor.hub is not gevent.get_hub() or reactor.hub.thread_ident)

    t = threading.Thread(target=thread)
    t.start()
    t.join(10)
    ok(result and result[0], 'hub of another thread, with a timer')
    ok(len(result) == 2 and result[1] != reactor.hub.thread_ident, 'not the hub of the main thread')

# Not in Mojolicious: the other greenlets run while the reactor waits in the hub, and the reactor runs in any greenlet
with subtest('Other greenlets while the reactor waits'):
    reactor = Pyjo.Reactor.Gevent.new()
    ticks = []

    def ticker():
        for _ in range(5):
            gevent.sleep(0.01)
            ticks.append(1)

    greenlet = gevent.spawn(ticker)
    reactor.timer(0.2, lambda reactor: reactor.stop())
    reactor.start()
    is_ok(len(ticks), 5, 'another greenlet ran')
    greenlet.join()

with subtest('Reactor in another greenlet, stopped from the main one'):
    reactor = Pyjo.Reactor.Gevent.new()
    reactor.timer(10, lambda reactor: None)
    greenlet = gevent.spawn(reactor.start)
    gevent.sleep(0.05)
    ok(reactor.is_running, 'running in another greenlet')
    started = time.monotonic()
    reactor.stop()
    greenlet.join(5)
    ok(greenlet.dead, 'stopped')
    ok(time.monotonic() - started < 1, 'woken up at once')
    reactor.reset()

# A callback of the reactor runs in the greenlet that runs the reactor, and waits for a function of gevent there
with subtest('Callback that waits for gevent'):
    reactor = Pyjo.Reactor.Gevent.new()
    order = []
    greenlets = []

    def sleeping(reactor):
        greenlets.append(gevent.spawn(lambda: order.append('greenlet')))
        order.append('sleep')
        gevent.sleep(0.05)
        order.append('woken')

    reactor.timer(0, sleeping)
    reactor.timer(0.01, lambda reactor: (order.append('timer'), reactor.stop()))
    reactor.start()
    is_deeply_ok(order, ['sleep', 'greenlet', 'woken', 'timer'], 'other greenlets ran while the callback waited')
    gevent.joinall(greenlets)

# What the reactor is for: the server of Pyjoyment and blocking requests of its user agent in the greenlets of the
# application, each with a reactor of its own on the same hub, which wait for their responses at the same time
with subtest('Blocking requests in greenlets'):
    app = Pyjoyment.new()
    app.log.level = 'fatal'

    @app.routes.get('/slow')
    def slow(c):
        c.render_later()
        Pyjo.IOLoop.timer(0.3, lambda loop: c.render(text='slow'))

    with Pyjo.Server.Daemon.new(app=app, listen=['http://127.0.0.1'], silent=True) as daemon:
        port = daemon.start().ports()[0]
        server = gevent.spawn(Pyjo.IOLoop.start)

        def request():
            with Pyjo.UserAgent.new() as ua:
                return ua.get(f'http://127.0.0.1:{port}/slow').result.text

        started = time.monotonic()
        greenlets = [gevent.spawn(request) for _ in range(5)]
        gevent.joinall(greenlets, timeout=10)
        elapsed = time.monotonic() - started
        Pyjo.IOLoop.stop()
        server.join(5)
    is_deeply_ok([greenlet.value for greenlet in greenlets], ['slow'] * 5, 'right responses')
    ok(elapsed < 1, 'at the same time')
    ok(server.dead, 'server stopped')

# The hub runs the callbacks of the loop, where the reactor cannot wait, which gevent tells
with subtest('Reactor in the hub'):
    reactor = Pyjo.Reactor.Gevent.new()
    reactor.timer(10, lambda reactor: None)
    errors = []
    reactor.hub.loop.run_callback(lambda: errors.append(eval_(reactor.one_tick)))
    gevent.sleep(0.01)
    isa_ok(errors and errors[0], gevent.exceptions.BlockingSwitchOutError, 'error of gevent')
    ok(not reactor.is_running, 'not running')
    reactor.reset()

# The handles of a reactor that does not run are not watched until it runs, as the loop would report a handle that is
# ready on every iteration, which the other greenlets make
with subtest('Ready handle while the reactor does not run'):
    a, b = socket.socketpair()
    reactor = Pyjo.Reactor.Gevent.new()
    calls = []
    reactor.io(a, lambda reactor, writable: calls.append(writable)).watch(a, False, True)
    started = time.process_time()
    gevent.sleep(0.2)
    ok(time.process_time() - started < 0.1, 'no busy loop of the hub')
    is_deeply_ok(calls, [], 'no callback')
    reactor.one_tick()
    is_deeply_ok(calls, [True], 'callback when it runs')
    reactor.one_tick()
    is_deeply_ok(calls, [True, True], 'watched again')
    reactor.reset()
    a.close()
    b.close()

# The events of one iteration of the loop in the order of the reactor of poll: the handles in the order in which they
# were added, where libev calls them in the reverse order, then the timers, the one of no time that a callback of I/O
# adds among them
with subtest('Order of the events'):
    # The handle that is added first has the higher number, as the backend of libev may report them by their numbers
    c, d = socket.socketpair()
    a, b = socket.socketpair()
    reactor = Pyjo.Reactor.Gevent.new()
    order = []

    def first(reactor, writable):
        order.append('first')
        reactor.next_tick(lambda reactor: order.append('next tick'))
        reactor.watch(a, False, False)

    def second(reactor, writable):
        order.append('second')
        reactor.watch(c, False, False)

    reactor.io(a, first).watch(a, False, True)
    reactor.io(c, second).watch(c, False, True)
    reactor.one_tick()
    is_deeply_ok(order, ['first', 'second', 'next tick'], 'right order')
    reactor.reset()
    for sock in (a, b, c, d):
        sock.close()

# The callbacks of the loop refer to the reactor weakly, as the loop lives as long as the thread
with subtest('Reactor freed'):
    a, b = socket.socketpair()
    reactor = Pyjo.Reactor.Gevent.new()
    reactor.io(a, lambda reactor, writable: None)
    reactor.timer(10, lambda reactor: None)
    reactor.recurring(1, lambda reactor: None)
    ref = weakref.ref(reactor)
    del reactor
    ok(ref() is None, 'freed without the garbage collector')
    gevent.sleep(0.01)
    a.close()
    b.close()

# Not in Mojolicious: the child of a fork, as a worker of Pyjo.Server.Prefork, reinitializes the hub, which gevent does
# for gevent.os.fork only
with subtest('Fork'):
    if not hasattr(os, 'fork'):
        plan(skip_all='os.fork is required')
    reactor = Pyjo.Reactor.Gevent.new()
    a, b = socket.socketpair()
    read, write = os.pipe()
    pid = os.fork()
    if not pid:
        os.close(read)
        try:
            fired = []
            reactor.io(a, lambda reactor, writable: fired.append(a.recv(1))).watch(a, True, False)
            reactor.timer(0.01, lambda reactor: b.send(b'x'))
            reactor.timer(5, lambda reactor: reactor.stop())
            reactor.recurring(0.01, lambda reactor: fired and reactor.stop())
            reactor.start()
            status = 0 if fired == [b'x'] else 1
        except BaseException:
            status = 2
        os.write(write, str(status).encode())
        os._exit(status)
    os.close(write)
    with os.fdopen(read) as file:
        child_status = file.read()
    _, status = os.waitpid(pid, 0)
    is_ok(child_status, '0', 'handle and timers in the child')
    is_ok(os.waitstatus_to_exitcode(status), 0, 'child exited')
    reactor.timer(0.01, lambda reactor: reactor.stop())
    reactor.start()
    ok(True, 'timer in the parent')
    a.close()
    b.close()

# Not in Mojolicious: the wakeup fd of the signals that the program sets, as a worker of Pyjo.Server.Prefork does, whose
# handler only notes the signal, which a watcher of the socket of the wakeup fd handles; gevent sets none of its own
with subtest('Wakeup fd of the signals'):
    if not hasattr(signal, 'SIGUSR1'):
        plan(skip_all='SIGUSR1 is required')
    noted = []
    woken = []
    handler = signal.signal(signal.SIGUSR1, lambda signum, frame: noted.append(signum))
    waker, wakee = socket.socketpair()
    waker.setblocking(False)
    wakee.setblocking(False)
    wakeup = signal.set_wakeup_fd(waker.fileno())
    try:
        reactor = Pyjo.Reactor.Gevent.new()

        def wake(reactor, writable):
            wakee.recv(4096)
            if noted:
                woken.extend(noted)
                reactor.stop()

        reactor.io(wakee, wake).watch(wakee, True, False)
        reactor.timer(0.1, lambda reactor: os.kill(os.getpid(), signal.SIGUSR1))
        reactor.timer(5, lambda reactor: reactor.stop())
        started = time.monotonic()
        reactor.start()
        elapsed = time.monotonic() - started
        reactor.reset()
    finally:
        signal.set_wakeup_fd(wakeup)
        signal.signal(signal.SIGUSR1, handler)
        waker.close()
        wakee.close()
    is_deeply_ok(woken, [signal.SIGUSR1], 'socket of the wakeup fd written to on the signal, after the handler')
    ok(elapsed < 1, 'at once')

# Not in Mojolicious: libuv sets a handle that it watches to the non-blocking mode, which a terminal shares with the shell
with subtest('Blocking mode of a handle'):
    read, write = os.pipe()
    reactor = Pyjo.Reactor.Gevent.new()
    reactor.io(read, lambda reactor, writable: None).watch(read, True, False)
    reactor.io(write, lambda reactor, writable: None).watch(write, False, True)
    ok(os.get_blocking(read) and os.get_blocking(write), 'blocking while watched')
    reactor.remove(read)
    reactor.remove(write)
    ok(os.get_blocking(read) and os.get_blocking(write), 'blocking after')
    os.close(read)
    os.close(write)

# Not in Mojolicious: on an error of a handle, libuv of Linux calls its writer once and does not watch it anymore, where
# the reactor of poll calls it for as long as the error lasts
with subtest('Writer of a pipe whose reader has closed it'):
    read, write = os.pipe()
    os.set_blocking(write, False)
    reactor = Pyjo.Reactor.Gevent.new()
    calls = []

    def writer(reactor, writable):
        calls.append(writable)
        if len(calls) == 1:
            os.close(read)

    reactor.io(write, writer).watch(write, False, True)
    reactor.timer(0.1, lambda reactor: reactor.stop())
    reactor.start()
    ok(len(calls) > 2, 'writer called again after the error')
    reactor.remove(write)
    os.close(write)

# Not in Mojolicious: a program of gevent.monkey, whose threads are greenlets, as the thread of the singleton of
# Pyjo.IOLoop, so in a process of its own
with subtest('gevent.monkey'):
    code = """
from gevent import monkey

monkey.patch_all()

import os

os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Gevent'

import gevent
import Pyjo.HelloWorld
import Pyjo.IOLoop
import Pyjo.Server.Daemon
import Pyjo.UserAgent

with Pyjo.Server.Daemon.new(app=Pyjo.HelloWorld.new(), listen=['http://127.0.0.1'], silent=True) as daemon:
    port = daemon.start().ports()[0]
    loop = Pyjo.IOLoop.singleton

    def request():
        with Pyjo.UserAgent.new() as ua:
            return ua.get(f'http://127.0.0.1:{port}/').result.text

    greenlets = [gevent.spawn(request) for _ in range(3)]
    gevent.spawn(lambda: (gevent.joinall(greenlets), loop.stop()))
    loop.start()
    print([greenlet.value for greenlet in greenlets])
    print(gevent.spawn(lambda: Pyjo.IOLoop.singleton is loop).get())
"""
    result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=60)
    is_ok(
        result.stdout,
        "['Your Pyjo is working!', 'Your Pyjo is working!', 'Your Pyjo is working!']\nFalse\n",
        'requests',
    )
    is_ok(result.stderr, '', 'nothing on the standard error')

done_testing()
