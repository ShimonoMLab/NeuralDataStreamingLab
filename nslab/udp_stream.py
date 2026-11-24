"""UDP streaming example for synthetic MEA data (localhost experiments).

Frames are application-level messages split into modest datagrams. The UDP
receiver verifies each fragment (length and CRC-32), detects duplicates and
reordering, and counts bytes of *actually received* payload. UDP itself does
not provide delivery, sequencing, or flow control; this example does not
implement reliable retransmission or congestion control.
"""
import socket
import time
import zlib

import numpy as np

from .config import StreamConfig
from .signal import SyntheticMEA
from . import framing, metrics, proc
from .tcp_stream import _check_host


def udp_receiver(sock, cfg: StreamConfig, process_ms=0.0, idle_timeout=1.0,
                 keep_payload=False, out=None):
    """Receive datagrams into a chunk/fragment map, stopping at end marker.

    This teaching example bounds valid sequence numbers to the expected run;
    it is NOT a general-purpose receiver for untrusted internet traffic.
    """
    sock.settimeout(idle_timeout)
    got = {}                # seq -> {fragment index: bytes or payload length}
    meta = {}               # seq -> (n_frag, capture timestamp)
    complete_at = {}        # seq -> arrival time of the last received fragment
    order_key = -1
    reordered = duplicates = datagrams = invalid = crc_failures = 0
    payload_bytes_received = 0
    rq = []

    while True:
        try:
            data, _ = sock.recvfrom(65535)
        except socket.timeout:
            break
        datagrams += 1
        if len(data) < framing.HEADER.size:
            invalid += 1
            continue
        try:
            h = framing.unpack_header(data[:framing.HEADER.size])
        except Exception:
            # An unexpected datagram must not terminate a classroom run.
            invalid += 1
            continue
        payload = data[framing.HEADER.size:]
        if h["payload_len"] != len(payload):
            invalid += 1
            continue
        if (zlib.crc32(payload) & 0xffffffff) != h["crc32"]:
            crc_failures += 1
            continue
        if h["last"] and h["payload_len"] == 0:
            break
        if (h["last"] or h["seq"] >= cfg.n_chunks or h["n_frag"] <= 0
                or h["frag"] >= h["n_frag"] or h["n_ch"] != cfg.channels
                or h["n_samp"] != cfg.samples_per_chunk):
            invalid += 1
            continue
        old = meta.get(h["seq"])
        if old is not None and old != (h["n_frag"], h["t_capture_ns"]):
            invalid += 1
            continue
        frags = got.setdefault(h["seq"], {})
        if h["frag"] in frags:
            duplicates += 1
            continue
        key = h["seq"] * 65536 + h["frag"]
        if key < order_key:
            reordered += 1
        order_key = max(order_key, key)
        frags[h["frag"]] = payload if keep_payload else len(payload)
        payload_bytes_received += len(payload)
        meta[h["seq"]] = (h["n_frag"], h["t_capture_ns"])
        rq.append(metrics.recv_queue_bytes(sock))
        if len(frags) == h["n_frag"]:
            complete_at[h["seq"]] = metrics.now_ns()
            if process_ms:
                time.sleep(process_ms / 1000)

    res = {"got": got, "meta": meta, "complete_at": complete_at,
           "reordered": reordered, "duplicates": duplicates,
           "datagrams": datagrams, "invalid_datagrams": invalid,
           "crc_failures": crc_failures,
           "payload_bytes_received": payload_bytes_received,
           "rq_bytes": rq, "t_last_ns": metrics.now_ns()}
    if out is not None:
        out.update(res)
    return res


def _pace_until(deadline_ns):
    """Sleep for most of the wait, then yield/spin briefly for precision.

    This is best-effort pacing by the OS, not a real-time timing guarantee.
    """
    while True:
        remaining = deadline_ns - metrics.now_ns()
        if remaining <= 0:
            return
        if remaining > 300_000:
            time.sleep((remaining - 100_000) / 1e9)
        elif remaining > 100_000:
            time.sleep(0)
        # Short final waits may busy-spin; this avoids full-interval spinning.


def udp_sender(host, port, cfg: StreamConfig, rate_factor=1.0, max_payload=1200,
               drop_prob=0.0, allow_lan=False, seed=1):
    """Send fragments with a target *payload* rate.

    drop_prob deliberately skips some sends to demonstrate missing fragments.
    It does not model actual network loss. Only use on authorised hosts.
    """
    _check_host(host, allow_lan)
    sample_bytes = np.dtype(cfg.dtype).itemsize
    if not (0 < max_payload <= 1200) or max_payload % sample_bytes:
        raise ValueError("max_payload must be 1..1200 and a multiple of sample bytes")
    if rate_factor <= 0 or not (0 <= drop_prob <= 1):
        raise ValueError("rate_factor must be positive and drop_prob between 0 and 1")

    mea = SyntheticMEA(cfg)
    rng = np.random.default_rng(seed)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target_Bps = cfg.bytes_per_second() * rate_factor
    t0 = metrics.now_ns()
    next_ns = float(t0)
    sent = skipped = 0
    try:
        for k in range(cfg.n_chunks):
            # Ideal acquisition time for this chunk, separate from send time.
            cap = int(t0 + k * cfg.chunk_ms * 1e6 / rate_factor)
            payload = mea.chunk(k).tobytes()
            n_frag = (len(payload) + max_payload - 1) // max_payload
            if n_frag > 65535:
                raise ValueError("chunk requires more than 65535 UDP fragments")
            for f in range(n_frag):
                part = payload[f * max_payload:(f + 1) * max_payload]
                datagram = framing.pack(k, cap, cfg.channels, cfg.samples_per_chunk,
                                        part, frag=f, n_frag=n_frag)
                _pace_until(int(next_ns))
                if drop_prob and rng.random() < drop_prob:
                    skipped += 1
                else:
                    sock.sendto(datagram, (host, port))
                    sent += len(datagram)
                next_ns += len(part) / target_Bps * 1e9
        end = framing.pack(cfg.n_chunks, metrics.now_ns(), 0, 0, b"", last=True)
        for _ in range(5):
            sock.sendto(end, (host, port))
            time.sleep(0.01)
    finally:
        sock.close()
    return {"sent_bytes": sent, "skipped": skipped,
            "t0_ns": t0, "t_end_ns": metrics.now_ns(),
            "target_MBps": target_Bps / 1e6}


def run_udp(cfg: StreamConfig, rate_factor=1.0, process_ms=0.0, rcvbuf=None,
            max_payload=1200, drop_prob=0.0, keep_payload=False):
    """Run UDP sender and receiver on localhost (two processes)."""
    rs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if rcvbuf:
        rs.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    rs.bind(("127.0.0.1", 0))
    port = rs.getsockname()[1]
    p, q = proc.start(udp_receiver, rs, cfg,
                      process_ms=process_ms, keep_payload=keep_payload)
    time.sleep(0.05)
    tx = udp_sender("127.0.0.1", port, cfg, rate_factor=rate_factor,
                    max_payload=max_payload, drop_prob=drop_prob)
    rx = proc.result(p, q)
    rs.close()
    return summarize_udp(cfg, tx, rx, max_payload), tx, rx


def reconstruct(cfg, rx, max_payload=1200):
    """Reassemble int16 waveforms; keep a mask for lost fragments.

    Requires run_udp(..., keep_payload=True). Missing samples are not
    interpolated: they are zero-filled for storage and separately masked.
    """
    n, ch = cfg.samples_per_chunk, cfg.channels
    dtype = np.dtype(cfg.dtype)
    X = np.zeros((cfg.n_chunks * n, ch), dtype=dtype)
    have = np.zeros(cfg.n_chunks * n * ch, bool)
    for seq, frags in rx["got"].items():
        if seq >= cfg.n_chunks:
            continue
        flat = X[seq * n:(seq + 1) * n].reshape(-1)
        base = seq * n * ch
        for f, part in frags.items():
            if not isinstance(part, bytes):
                raise ValueError("reconstruct() needs run_udp(..., keep_payload=True)")
            a = np.frombuffer(part, dtype=dtype)
            offset = f * max_payload // dtype.itemsize
            flat[offset:offset + a.size] = a
            have[base + offset: base + offset + a.size] = True
    return X, have.reshape(-1, ch)


def summarize_udp(cfg, tx, rx, max_payload):
    n_frag = (cfg.chunk_bytes + max_payload - 1) // max_payload
    expected = cfg.n_chunks * n_frag
    received = sum(len(v) for k, v in rx["got"].items() if k < cfg.n_chunks)
    complete = [s for s in rx["complete_at"] if s < cfg.n_chunks]
    lat = [(rx["complete_at"][s] - rx["meta"][s][1]) / 1e6
           for s in sorted(complete)]
    dur = (rx["t_last_ns"] - tx["t0_ns"]) / 1e9
    return {
        "protocol": "UDP", "target_MBps": tx["target_MBps"],
        "generation_MBps": cfg.bytes_per_second() / 1e6,
        "datagrams_expected": expected, "datagrams_received": int(received),
        "loss_rate": 1 - received / expected, "chunks_complete": len(complete),
        "chunks_sent": cfg.n_chunks,
        "reordered": rx["reordered"], "duplicates": rx["duplicates"],
        "invalid_datagrams": rx["invalid_datagrams"],
        "crc_failures": rx["crc_failures"],
        "goodput_MBps": rx["payload_bytes_received"] / max(dur, 1e-9) / 1e6,
        "latency_complete_chunks": metrics.summarize_latency(lat),
        "jitter_ms": metrics.interarrival_jitter_ms(lat) if len(lat) > 1 else None,
        "max_recv_queue_bytes": max([q for q in rx["rq_bytes"] if q is not None], default=None),
        "same_host_clock": True,
    }
