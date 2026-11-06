[![Home](https://img.shields.io/badge/%F0%9F%8F%A0-Pyjoyment-02a7a0)](https://pyjoyment.dev/)
[![GitHub](https://img.shields.io/github/stars/pyjoyment/Pyjo-Reactor-Gevent?style=flat&label=GitHub%20%E2%AD%90&logo=github)](https://github.com/pyjoyment/Pyjo-Reactor-Gevent)
[![Documentation](https://img.shields.io/badge/docs-Read_the_Docs-blue?logo=readthedocs&logoColor=white)](https://pyjoyment.readthedocs.io/projects/pyjo-reactor-gevent/en/stable/)
[![License](https://img.shields.io/github/license/pyjoyment/Pyjo-Reactor-Gevent.svg)](https://github.com/pyjoyment/Pyjo-Reactor-Gevent/blob/main/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/Pyjo-Reactor-Gevent?logo=pypi&logoColor=white)](https://pypi.org/project/Pyjo-Reactor-Gevent/)
[![Tests](https://github.com/pyjoyment/Pyjo-Reactor-Gevent/actions/workflows/tests.yaml/badge.svg?branch=main)](https://github.com/pyjoyment/Pyjo-Reactor-Gevent/actions/workflows/tests.yaml)
[![Trunk Check](https://github.com/pyjoyment/Pyjo-Reactor-Gevent/actions/workflows/trunk-check.yaml/badge.svg?branch=main)](https://github.com/pyjoyment/Pyjo-Reactor-Gevent/actions/workflows/trunk-check.yaml)

# Pyjo-Reactor-Gevent

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/pyjoyment/Pyjoyment/main/Pyjoyment/resources/public/pyjo/logo-white-4x.png">
    <img src="https://raw.githubusercontent.com/pyjoyment/Pyjoyment/main/Pyjoyment/resources/public/pyjo/logo-black-4x.png" alt="Pyjoyment" width="322">
  </picture>
</p>

Low level event reactor with [gevent](https://www.gevent.org) support for
[Pyjoyment](https://pyjoyment.dev/).

`Pyjo.Reactor.Gevent` is a reactor of Pyjoyment on the hub of gevent, whose
loop of libev or libuv watches the handles and runs the timers, so that the
servers of Pyjoyment, `daemon`, `prefork` and `prethread`, and its user agent
run along with the greenlets of a program of gevent.

## Installation

From [PyPI](https://pypi.org/project/Pyjo-Reactor-Gevent/), with
[Pyjoyment](https://pypi.org/project/Pyjoyment/) and
[gevent](https://pypi.org/project/gevent/), which it depends on:

```sh
pip install Pyjo-Reactor-Gevent
```

or, in a project managed with [uv](https://docs.astral.sh/uv/):

```sh
uv add Pyjo-Reactor-Gevent
```

The development version is installed from the Git repository:

```sh
pip install git+https://github.com/pyjoyment/Pyjo-Reactor-Gevent
```

## Usage

Select the reactor with `PYJO_REACTOR` before `Pyjo.IOLoop` is loaded:

```sh
PYJO_REACTOR=Pyjo.Reactor.Gevent python myapp.py daemon
PYJO_REACTOR=Pyjo.Reactor.Gevent python myapp.py prefork
```

The reactor runs in the greenlet that starts it, and the other greenlets of the
program run while it waits for events. A blocking request of the user agent
waits in the greenlet that makes it, without blocking the others.

gevent runs on libev by default. Its loop of libuv needs
[cffi](https://pypi.org/project/cffi/):

```sh
pip install Pyjo-Reactor-Gevent cffi
GEVENT_LOOP=libuv-cffi PYJO_REACTOR=Pyjo.Reactor.Gevent python myapp.py daemon
```

With `gevent.monkey.patch_all()`, the threads of the program are greenlets, so
each greenlet has a `Pyjo.IOLoop.singleton` of its own, and
`Pyjo.Server.Prethread` and `Pyjo.IOLoop.ChildProcess` do not work.

## Examples

`examples/hello_world_gevent.py` runs `Pyjo.HelloWorld` on the server of
Pyjoyment on gevent until the signal `INT` or `TERM`, in a `with` block, which
closes the listen sockets of the server at its end:

```python
import os


# The reactor of Pyjoyment on gevent, selected before Pyjo.IOLoop is loaded
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Gevent'

import Pyjo.HelloWorld  # noqa: E402
import Pyjo.Server.Daemon  # noqa: E402


# The server of Pyjoyment on gevent, until the signal INT or TERM, which closes its listen sockets at the end of the block
with Pyjo.Server.Daemon.new(app=Pyjo.HelloWorld.new(), listen=['http://*:3000']) as daemon:
    daemon.run()
```

```console
$ python examples/hello_world_gevent.py
Web application available at http://127.0.0.1:3000
```

```console
$ curl http://127.0.0.1:3000/
Your Pyjo is working!
```

`examples/loopback_gevent.py` is a server of Pyjoyment and its user agent on
the hub of gevent, a blocking request, which runs the event loop until the
response, and the class of the reactor and whether it runs on the hub of
gevent, which Build checks with the wheel:

```python
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
```

```console
$ python examples/loopback_gevent.py
Pyjo_Reactor_Gevent gevent
200 Your Pyjo is working!
```

`examples/greenlets_gevent.py` is a server of Pyjoyment in the main greenlet,
along with two greenlets of the application that request it, one with a socket
of gevent and one with a blocking request of the user agent of Pyjoyment:

```python
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
```

```console
$ python examples/greenlets_gevent.py
gevent.socket: HTTP/1.1 200 OK
Pyjo.UserAgent: 200 Your Pyjo is working!
```

## Documentation

The API of the module and the examples are on
[Read the Docs](https://pyjoyment.readthedocs.io/projects/pyjo-reactor-gevent/en/stable/).

## Status

A reactor of Pyjoyment in a distribution of its own, as
[Pyjo-Reactor-Twisted](https://github.com/pyjoyment/Pyjo-Reactor-Twisted) and
[Pyjo-Reactor-Uvloop](https://github.com/pyjoyment/Pyjo-Reactor-Uvloop).
