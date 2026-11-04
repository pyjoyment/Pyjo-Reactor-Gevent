.. automodule:: Pyjo.Reactor.Gevent
    :members:

.. The examples follow the API, after the docstring of the module, which ends with its classes: a section here would
   be a second title of the page, as the sections of the docstring are its own.

.. rubric:: Examples
    :heading-level: 2
    :name: examples

``examples/hello_world_gevent.py`` runs ``Pyjo.HelloWorld`` on the server of
Pyjoyment on gevent until the signal ``INT`` or ``TERM``, in a ``with`` block,
which closes the listen sockets of the server at its end:

    .. include:: ../examples/hello_world_gevent.py
       :code: python

.. code-block:: console

    $ python examples/hello_world_gevent.py
    Web application available at http://127.0.0.1:3000

.. code-block:: console

    $ curl http://127.0.0.1:3000/
    Your Pyjo is working!

``examples/loopback_gevent.py`` is a server of Pyjoyment and its user agent on
the hub of gevent, a blocking request, which runs the event loop until the
response, and the class of the reactor and whether it runs on the hub of
gevent:

    .. include:: ../examples/loopback_gevent.py
       :code: python

.. code-block:: console

    $ python examples/loopback_gevent.py
    Pyjo_Reactor_Gevent gevent
    200 Your Pyjo is working!

``examples/greenlets_gevent.py`` is a server of Pyjoyment in the main greenlet,
along with two greenlets of the application that request it, one with a socket
of gevent and one with a blocking request of the user agent of Pyjoyment:

    .. include:: ../examples/greenlets_gevent.py
       :code: python

.. code-block:: console

    $ python examples/greenlets_gevent.py
    gevent.socket: HTTP/1.1 200 OK
    Pyjo.UserAgent: 200 Your Pyjo is working!
