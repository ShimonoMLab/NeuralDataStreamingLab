"""Run a receiver in a separate process so that sender and receiver do
not share one Python interpreter lock (which would cause losses that
have nothing to do with the network)."""
import multiprocessing as mp


def _child(q, fn, args, kwargs):
    try:
        q.put(("ok", fn(*args, **kwargs)))
    except Exception as e:                       # surface errors to the parent
        q.put(("err", repr(e)))


def start(fn, *args, **kwargs):
    ctx = mp.get_context("fork")
    q = ctx.Queue()
    p = ctx.Process(target=_child, args=(q, fn, args, kwargs), daemon=True)
    p.start()
    return p, q


def result(p, q, timeout=120):
    status, val = q.get(timeout=timeout)
    p.join(timeout=10)
    if status != "ok":
        raise RuntimeError(val)
    return val
