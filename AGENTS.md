# Agent Guide: Pyjo-Reactor-Gevent

## Overview

`Pyjo.Reactor.Gevent` is a reactor of Pyjoyment on the hub of gevent, in a
distribution of its own, as every reactor of Pyjoyment with a dependency is:
Pyjoyment has no runtime dependencies, and one reactor does not pull the
dependencies of another. The conventions are those of Pyjoyment: read its
`AGENTS.md` in the checkout next to this one, `../Pyjoyment/AGENTS.md`. The
repository is made of the one of Pyjo-Reactor-Uvloop, itself made of the one
of Pyjo-Reactor-Twisted, whose workflows and linters it shares; their
`AGENTS.md`, in `../Pyjo-Reactor-Uvloop` and `../Pyjo-Reactor-Twisted`, say
what each of their loops needed.

## How it works

- It is a subclass of `Pyjo.Reactor.Poll`, as `Pyjo.Reactor.Twisted` is, whose
  watchers of I/O and timers are those of the loop of the hub of gevent,
  `reactor.hub.loop`: a `loop.io` for each handle and a `loop.timer` for each
  timer, of libev or of libuv, as `GEVENT_LOOP` selects. Unlike the reactor of
  Twisted, the loop is not unique in a process: any number of reactors share
  the hub of the thread that makes them, as each blocking user agent makes one
  of its own, and the threads of `Pyjo.Server.Prethread` have hubs of their
  own, as `gevent.get_hub()` is one for each thread.
- The loop calls the watchers in the hub, where a callback of Pyjoyment could
  not wait for anything of gevent, so the watchers only note the handle or the
  timer and wake the greenlet that waits for the reactor with `run_callback`,
  which gevent runs in the watcher prepare of the next iteration, before it
  polls again: the greenlet gets all the events of an iteration at once. The
  callbacks of Pyjoyment run in the greenlet that called `start` or
  `one_tick`, which waits with `hub.switch()`, so a callback can recurse into
  the reactor, as a blocking request does, or wait for gevent, and the other
  greenlets run meanwhile. `stop` wakes the greenlet up, also from another
  greenlet, and `_wake`, of the cleanups of finalizers from any thread, does
  with `run_callback_threadsafe`. gevent raises `BlockingSwitchOutError` when
  the reactor runs in the hub itself.
- `one_tick` follows the one of `Pyjo.Reactor.Poll`: it waits at most half a
  second, on a timer of the reactor, or for one iteration of the loop with a
  timer of no time when an event is ready already, as the poll of no time of
  the reactor of poll, then calls the callbacks of I/O, in the order in which
  the handles were added, and of the timers, in the order in which they were
  made. libev calls the watchers of an iteration in the reverse order of its
  backend, and libuv in its order: on Linux, a byte of the manager of
  `Pyjo.Server.Prethread` that stops the worker came before the connection
  that the worker had to accept, so it closed its listen socket without the
  request. A timer of no time, as `next_tick`, has no timer of the loop: it is
  due at once, so that one that a callback of I/O adds runs after the I/O of
  the same tick, as in the reactor of poll, and not after the I/O of the next
  iteration, which accepted a second connection before the limit of
  connections applied. libev does not repeat a timer with no interval, so a
  recurring timer of no time is due again at each tick too.
- The watchers of the loop are not referenced, so that a reactor that does not
  run does not keep a program of gevent alive; its timer of half a second is,
  while a greenlet waits, or the hub would end with `LoopExit`. A handle that
  is ready while nobody waits for the reactor is not watched until somebody
  does, as the loop would report it on each iteration that the other greenlets
  make and keep the hub busy.
- A timer is started with `update=True`, as libev and libuv take the time of
  the start of the iteration, which the callbacks may have taken long after,
  and with its time as its interval, so that `again` restarts it with
  `ev_timer_again` or `uv_timer_again`; the callback stops one that is not
  recurring.
- The callbacks of the loop refer to the reactor weakly, as the loop lives as
  long as the thread, and a finalizer stops the watchers of a reactor that is
  freed, in the thread of the hub, with `run_callback_threadsafe` from
  another one.
- `Pyjo.Reactor._finalize` defers the cleanup of a finalizer that runs below a
  frame of a reactor or of its event loop, since pyjoyment/Pyjoyment#534,
  which takes the file of the module of the class of the reactor,
  `Pyjo/Reactor/Gevent.py`, wherever it is installed, and the modules of
  `event_loop_modules`, which names `gevent`. The hub runs its loop in a
  greenlet of its own, whose frames are not below those of the greenlet of
  the reactor: a finalizer of `Pyjo.IOLoop.TLS` that the garbage collector
  ran in `_run_callbacks` of the loop of gevent, below `Hub.run` only, closed
  its socket in the middle of the callbacks of the loop, in
  `t/Pyjo/IOLoop_TLS.py` of Pyjoyment. `greenlet` is not named: it is a C
  extension, and a probe of `_finalize` over the suite of Pyjoyment on both
  loops found no frame of it below any of its calls (#2).
- The child of a fork, as a worker of `Pyjo.Server.Prefork`, calls
  `gevent.hub.reinit()`, from `os.register_at_fork`, which gevent does itself
  for `gevent.os.fork` only, the `os.fork` of `gevent.monkey` too, where it is
  not called again. libev of the child would share the epoll of the parent.
- The signals need no relay, as uvloop does: gevent sets no wakeup fd of its
  own, so the socket pair that a worker of `Pyjo.Server.Prefork` and
  `Pyjo.Server.Daemon` set gets the byte of the signal from the handler of C
  of Python, and the handler of Python runs before the callback of its
  watcher, which runs code of Python. libuv checks the signals only every 300
  milliseconds itself.
- libuv sets a handle to the non-blocking mode when it makes the watcher, and
  never back, which a terminal shares with the shell, so the reactor sets a
  handle that was blocking back after `loop.io`, as Pyjo-Reactor-Uvloop does.
- On an error of a handle, as a pipe whose reader has closed it, libuv of
  Linux stops the watcher and calls it once with a status, which gevent passes
  as events below zero, while it takes the watcher for active still, so the
  next start did nothing. The reactor takes the handle for readable and
  writable, as `Pyjo.Reactor.Poll` does, and starts the watcher again at the
  next wait, so that the callback is called for as long as the error lasts.
- The loops of cffi, `libev-cffi` and `libuv-cffi`, need cffi, which gevent
  depends on only on Windows, so the group `dev` has it, and the matrix of
  Tests runs both `libev-cext` and `libuv-cffi`. gevent 26.9.0 has wheels for
  CPython 3.12 to 3.15, with no free-threaded build and no PyPy, so the matrix
  has neither, unlike the one of Pyjoyment.
- With `gevent.monkey.patch_all()`, `threading` takes a greenlet for a thread,
  so `Pyjo.IOLoop.singleton` and the deferred cleanups of `Pyjo.Reactor` are
  of each greenlet, and the threads of `Pyjo.Server.Prethread` are greenlets,
  which hang. `subprocess` and the handler of `SIGCHLD` of gevent reap the
  children, so `Pyjo.IOLoop.ChildProcess` does not get their status. The rest
  of the suite of Pyjoyment passes with it, and a subtest of
  `t/Reactor_Gevent.py` runs a server and blocking requests in greenlets with
  it, but CI does not run the suite with it.

## Development

The distribution depends on the release of Pyjoyment on PyPI, with the lowest
version that has what it uses, which `uv.lock` pins. A change of Pyjoyment that
is not released yet would be its Git repository in `[tool.uv.sources]` of
`pyproject.toml`, until its release, and not a path to the checkout next to
this one: uv applies the sources of a dependency of Git, so whoever installs
the reactor from Git would get a subdirectory of this repository that does not
exist. Before a release of the reactor, the source goes and the lowest
version of `dependencies` becomes the release of Pyjoyment that has the
change, as a wheel carries no sources of uv.
The tests lay the checkout next to this one over it with `--with-editable`,
so that they test what is there:

```sh
mise exec -- uv sync
mise exec -- uv run --with-editable ../Pyjoyment python -m Pyjo.Prove -l -r t
```

Perl's `prove` runs them too, with `.proverc`, under `uv run`, whose `python`
is the one of the environment, where Pyjoyment and the reactor are installed,
so that it needs no `PYTHONPATH`, unlike in Pyjoyment:

```sh
mise exec -- uv run --with-editable ../Pyjoyment prove -r t
```

The tests of Pyjoyment run on this reactor as well, with `PYJO_REACTOR`:

```sh
cd ../Pyjoyment
PYJO_REACTOR=Pyjo.Reactor.Gevent mise exec -- uv run --project ../Pyjo-Reactor-Gevent --with-editable . \
  python -m Pyjo.Prove -l -j 4 -r t
```

The workflow Tests runs the tests of the reactor against the release of
`uv.lock`, without `--with-editable`, so that a pull request tests what an
installation gets, and the suite of Pyjoyment of the tag of that release on
the reactor, or of its commit when `uv.lock` has it from Git. A dispatch
with `pyjoyment-ref`, a branch, a tag or a commit of Pyjoyment, installs
its Pyjoyment of Git over it with `uv run --with`, not editable, as an
installation from Git gets it, and runs the suite of its checkout, so that
what the next release of Pyjoyment breaks is known before the release. It
ends with the job `Tests`, the one check of it that the ruleset of `main`
requires, as in Pyjoyment: it fails when a job of the matrix failed or was
cancelled, so a new version of Python needs no change of the ruleset.

Trunk checks the files with the linters of Pyjoyment, from `.trunk/trunk.yaml`:

```sh
trunk check --all
```

actionlint is the one of the maintained fork kjanat/actionlint, whose releases
continue those of rhysd/actionlint after 1.7.12, as in Pyjoyment: `downloads`
in `.trunk/trunk.yaml` replaces the URLs of the plugin with those of the fork,
whose files have the same names.

Biome formats and lints JSON, Markdown and YAML, with `biome.jsonc`, in the
place of prettier, markdownlint and yamlfmt, as in Pyjoyment, and yamllint
checks what Biome does not, as the duplicated keys. The version of Biome is
the pinned one of `.trunk/trunk.yaml`, where `!` keeps it from
`trunk upgrade`, and which Renovate bumps: the repository has no
`package.json` of its own. Renovate cannot run `biome migrate` nor
`biome format`, so the workflow Biome Migrate runs them with
`npx --yes @biomejs/biome@<version>` of that version, migrates `biome.jsonc`
and formats the files that the new version formats otherwise, which Trunk Check
would fail on, on the branch of Renovate, and commits them with the token of
the workflow, as the author that `gitIgnoredAuthors` of `.renovaterc.json`
names, so that Renovate keeps updating the branch, and starts the checks of the
commit with `workflow_dispatch`; anywhere else, a file that this Biome would
migrate or format fails the check. The token cannot change a workflow, so a
file of `.github/workflows` that the new Biome formats otherwise fails the push
of the commit, and is formatted by hand. The definition of
Biome in `.trunk/trunk.yaml` is the one of Pyjoyment, which explains why it is
not the one of the plugin of Trunk.

Its pyright needs the environment of `uv sync`, `.venv`, which `tool.pyright` of
`pyproject.toml` names, as the reactor imports Pyjoyment and gevent, while
Pyjoyment has no dependencies to find; Trunk Check of CI runs `uv sync` first.
The examples and the tests, and `docs/conf.py`, are not checked by pyright: for them it takes the
regular package `Pyjo` of Pyjoyment, which hides `Pyjo.Reactor.Gevent` of the
checkout.

The documentation is one page, `docs/index.rst`: the docstrings of the
module, which autodoc imports, and the examples from `examples/`, included,
never copied, as in Pyjoyment. Read the Docs installs it with `uv sync` of `uv.lock` and the
group `docs`, not with pip and a `docs/requirements.txt` as Pyjoyment does,
so that it builds with the versions of the lock, as the workflow Docs does.
The links to the modules of Pyjoyment come from intersphinx, to its stable
version from the stable one, and to latest from the others. The examples are
a rubric, not a section: they follow the docstring of the module, whose
sections are its own, so that a section would be a second title of the page.
The workflow Docs builds it with `-W`, as Read the Docs does, with the
Pyjoyment of `uv.lock`:

```sh
mise exec -- uv run --group docs --with-editable ../Pyjoyment sphinx-build -W --keep-going -b html docs docs/_build/html
```

`t/Reactor_Gevent.py` is `t/Pyjo/Reactor_Poll.py` of Pyjoyment, the port of
`t/mojo/reactor_poll.t` of Mojolicious, in the subtests of 9.53, with subtests
of its own for the hub of gevent: the hub of a thread, the other greenlets
while the reactor waits, a reactor in another greenlet, a callback that waits
for gevent, blocking requests in greenlets, the reactor in the hub, a ready
handle while the reactor does not run, the order of the events, a reactor that
is freed, the child of a fork, the wakeup fd of the signals, the blocking mode
of a handle, the writer of a pipe whose reader has closed it, and
`gevent.monkey`. CI runs it on both loops.
`t/docs/Reactor_Gevent.py` runs the synopsis of the module, in both forms, as
`t/docs` of Pyjoyment does.

## Releases

The releases are those of Pyjoyment, with its workflows: a release is a tag of
the version of `pyproject.toml`, without a `v`, as `0.2.0`, which Automated
version sets with `uv version`, the components of its `--bump` from a list, as
`patch dev` or `minor rc`, or the version of `manual`, in the pull request of
the branch `automated/version`. After a release, a pre-release goes with the
release that it comes before, as PEP 440 orders `0.2.0.dev1` before `0.2.0`,
so `dev` alone after `0.2.0` fails. Automated tag tags the merge commit of
that pull request when it is merged, or with a comment `/tag` of a maintainer
on it, and starts Release on the tag. A development version, as `0.2.1.dev1`,
is not tagged. The version is static in `pyproject.toml`, as `uv version`
cannot set a dynamic one, and `__version__` of the module reads it from the
metadata of the distribution.

Each release has a milestone of GitHub named after its version, as `0.2.1`,
which holds the pull requests and the issues that go into it, so that the
milestone says what the diff and the notes of the release have. A pull
request that a maintainer or an agent opens gets the milestone of the next
release when it is opened, as `gh pr create --milestone 0.2.1`, and so does
the issue that it closes, as the change is then on its way into that
release. An issue alone waits for its triage without a milestone, and so
does a pull request of Renovate or of a workflow, as Automated version,
which none of them gives one. After a release, its milestone is closed, and
the milestone of the next release is named after the version of
`pyproject.toml` without its pre-release, as `0.2.1` of `0.2.1.dev1`, or the
next patch release while the version is still the one released, and is
renamed when a bump changes it.

Release builds the sdist and the wheel, attests their build provenance with
`actions/attest`, and makes a draft of the GitHub release with them, whose
notes are rewritten before it is published, and an SBOM in SPDX, attested
with them. The module bundles nothing, unlike the package of Pyjoyment, whose
SBOM lists the JavaScript libraries of its package, so the SBOM is what an
installation of the wheel gets: the distribution and its dependencies, as
Pyjoyment, gevent and greenlet, at the versions of `uv.lock`, installed in a virtual
environment of their own, which Syft scans. Syft on `uv.lock` itself would
take its dependency groups too, as Sphinx, which an installation does not get.
The sdist does not carry the SBOM, as the one of Pyjoyment does, as it is made
of the wheel, which is built after the sdist.

The workflow PyPI, run manually, publishes the files of the latest published
release, or of the release of the tag of its input, on PyPI once their
attestations of Release verify, in the environment `publishing`, which waits
for an approval and takes `main` and tags only. The trusted publisher of PyPI
names `pypi.yaml` and that environment.

```sh
gh attestation verify pyjo_reactor_gevent-0.2.0.tar.gz -R pyjoyment/Pyjo-Reactor-Gevent
```
