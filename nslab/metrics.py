"""Measurement helpers.

Latency here is computed from the sender's capture timestamp and the
receiver's arrival time. That is only meaningful when both use the same
clock, i.e. both ends run on the same host (localhost). Across hosts
without clock synchronisation, report round-trip time instead.
"""
import sys, fcntl, struct, socket, time
import numpy as np

try:
    import termios
    _TIOCOUTQ = termios.TIOCOUTQ
    _FIONREAD = termios.FIONREAD
except (ImportError, AttributeError):        # non-Linux
    _TIOCOUTQ = _FIONREAD = None


def now_ns():
    return time.perf_counter_ns()


def recv_queue_bytes(sock):
    """Bytes waiting in the kernel receive buffer (Linux FIONREAD)."""
    if _FIONREAD is None:
        return None
    return struct.unpack("i", fcntl.ioctl(sock.fileno(), _FIONREAD, b"\0\0\0\0"))[0]


def send_queue_bytes(sock):
    """Bytes not yet acknowledged/sent in the kernel send buffer (Linux TIOCOUTQ)."""
    if _TIOCOUTQ is None or not sys.platform.startswith("linux"):
        return None
    return struct.unpack("i", fcntl.ioctl(sock.fileno(), _TIOCOUTQ, b"\0\0\0\0"))[0]


def summarize_latency(lat_ms):
    a = np.asarray(lat_ms, float)
    if a.size == 0:
        return {}
    return {"median_ms": float(np.median(a)), "p95_ms": float(np.percentile(a, 95)),
            "p99_ms": float(np.percentile(a, 99)), "max_ms": float(a.max())}


def interarrival_jitter_ms(transit_ms):
    """RFC 3550 style running estimate J += (|D| - J)/16 over successive transit times."""
    j = 0.0
    t = np.asarray(transit_ms, float)
    for d in np.abs(np.diff(t)):
        j += (d - j) / 16.0
    return float(j)


def process_usage():
    try:
        import resource
        r = resource.getrusage(resource.RUSAGE_SELF)
        return {"cpu_user_s": r.ru_utime, "cpu_sys_s": r.ru_stime, "max_rss_MB": r.ru_maxrss / 1024}
    except Exception:
        return {}
