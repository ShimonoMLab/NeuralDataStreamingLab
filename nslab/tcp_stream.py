"""TCP sender and receiver for synthetic MEA chunks.

The sender "captures" one chunk every chunk_ms (real-time pacing) or as
fast as it can, stamps it, frames it and writes it with sendall(). The
receiver reads exactly one frame at a time with read_frame(), optionally
spends process_ms on each frame (a slow analysis step), and records
arrival time, CRC, sequence continuity and kernel queue sizes.
"""
import socket, time
import numpy as np
from .config import StreamConfig
from .signal import SyntheticMEA
from . import framing, metrics, proc

LOOPBACK = {"127.0.0.1", "::1", "localhost"}


def _check_host(host, allow_lan):
    if host not in LOOPBACK and not allow_lan:
        raise PermissionError("Non-loopback host. Use allow_lan=True only on networks you are "
                              "authorised to test, and never load-test someone else's network.")


def tcp_receiver(listen_sock, cfg: StreamConfig, process_ms=0.0, rcvbuf=None,
                 keep_payload=False, record_recv_sizes=False, out=None):
    """Accept one connection on listen_sock and receive until the last frame."""
    conn, _ = listen_sock.accept()
    if rcvbuf:
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    res = {"seq": [], "arrival_ns": [], "t_capture_ns": [], "crc_ok": [], "rq_bytes": [],
           "recv_sizes": [] if record_recv_sizes else None, "payloads": {} if keep_payload else None}
    t_first = None
    while True:
        h, payload = framing.read_frame(conn, res["recv_sizes"])
        t = metrics.now_ns()
        t_first = t_first or t
        res["seq"].append(h["seq"]); res["arrival_ns"].append(t); res["t_capture_ns"].append(h["t_capture_ns"])
        res["crc_ok"].append(h["crc_ok"]); res["rq_bytes"].append(metrics.recv_queue_bytes(conn))
        if keep_payload:
            res["payloads"][h["seq"]] = payload
        if process_ms:
            time.sleep(process_ms / 1000)
        if h["last"]:
            break
    res["t_first_ns"], res["t_last_ns"] = t_first, metrics.now_ns()
    conn.close()
    if out is not None:
        out.update(res)
    return res


def tcp_sender(host, port, cfg: StreamConfig, pace=True, sndbuf=None, allow_lan=False, rate_factor=1.0):
    _check_host(host, allow_lan)
    mea = SyntheticMEA(cfg)
    s = socket.create_connection((host, port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if sndbuf:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)
    n = cfg.n_chunks
    period = cfg.chunk_ms * 1e6 / rate_factor
    sent_bytes = 0; block_ms = []; sq = []; t_cap = []
    t0 = metrics.now_ns()
    for k in range(n):
        due = t0 + k * period
        if pace:
            while metrics.now_ns() < due:
                time.sleep(max(0.0, (due - metrics.now_ns()) / 1e9 - 0.0002))
        # Ideal acquisition timeline, not the actual time of a real ADC capture.
        # When sending blocks, the scheduled sample age still accumulates.
        cap = int(due) if pace else metrics.now_ns()
        x = mea.chunk(k)
        frame = framing.pack(k, cap, cfg.channels, cfg.samples_per_chunk, x.tobytes(), last=(k == n - 1))
        tb = metrics.now_ns()
        s.sendall(frame)
        block_ms.append((metrics.now_ns() - tb) / 1e6)
        sq.append(metrics.send_queue_bytes(s)); t_cap.append(cap)
        sent_bytes += len(frame)
    t_end = metrics.now_ns()
    s.close()
    return {"rate_factor": rate_factor if pace else None, "sent_bytes": sent_bytes, "t0_ns": t0, "t_end_ns": t_end, "send_block_ms": block_ms,
            "send_queue_bytes": sq, "t_capture_ns": t_cap}


def run_tcp(cfg: StreamConfig, pace=True, process_ms=0.0, rcvbuf=None, sndbuf=None,
            keep_payload=False, record_recv_sizes=False, rate_factor=1.0):
    """Receiver in a child process, sender in this process, over loopback."""
    ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if rcvbuf:
        ls.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)   # set before listen to bound the window
    ls.bind(("127.0.0.1", 0)); ls.listen(1)
    port = ls.getsockname()[1]
    p, q = proc.start(tcp_receiver, ls, cfg, process_ms=process_ms, rcvbuf=rcvbuf,
                      keep_payload=keep_payload, record_recv_sizes=record_recv_sizes)
    tx = tcp_sender("127.0.0.1", port, cfg, pace=pace, sndbuf=sndbuf, rate_factor=rate_factor)
    rx = proc.result(p, q); ls.close()
    return summarize_tcp(cfg, tx, rx), tx, rx


def summarize_tcp(cfg, tx, rx):
    lat = (np.array(rx["arrival_ns"]) - np.array(rx["t_capture_ns"])) / 1e6
    seq = np.array(rx["seq"])
    payload_bytes = len(seq) * cfg.chunk_bytes
    dur = (rx["t_last_ns"] - tx["t0_ns"]) / 1e9
    return {
        "protocol": "TCP", "chunks_sent": cfg.n_chunks, "chunks_received": int(len(seq)),
        "in_order": bool(np.all(np.diff(seq) == 1)), "crc_all_ok": bool(all(rx["crc_ok"])),
        "offered_MBps": cfg.bytes_per_second() / 1e6 * (tx.get("rate_factor") or 1.0),
        "goodput_MBps": payload_bytes / dur / 1e6,
        "latency": metrics.summarize_latency(lat),
        "jitter_ms": metrics.interarrival_jitter_ms(lat),
        "max_send_block_ms": float(max(tx["send_block_ms"])),
        "max_send_queue_bytes": max([q for q in tx["send_queue_bytes"] if q is not None], default=None),
        "max_recv_queue_bytes": max([q for q in rx["rq_bytes"] if q is not None], default=None),
        "same_host_clock": True,
    }
