"""Five classroom experiments on localhost.

  E1  bandwidth arithmetic: channels x fs x bytes, and transfer times
  E2  TCP is a byte stream: how recv() splits frames, and why framing
  E3  offered load sweep: TCP vs UDP goodput, loss and latency
  E4  slow receiver: TCP back-pressure vs UDP loss
  E5  what loss does to the waveform and to spike detection

Usage:  python -m nslab.experiments all  [--out results]
        python -m nslab.experiments E4
"""
import os, sys, json, socket, argparse
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from .config import StreamConfig, bandwidth_table, required_bandwidth, transfer_time
from .signal import SyntheticMEA
from .tcp_stream import run_tcp, tcp_sender
from .udp_stream import run_udp, reconstruct
from . import framing, proc, metrics


def e1(out):
    rows = bandwidth_table()
    vol130 = 130e9
    check = {
        "report_GB_per_hour": 130,
        "hours_at_100Mbps": transfer_time(vol130, 100e6) / 3600,
        "minutes_at_1Gbps": transfer_time(vol130, 1e9) / 60,
        "Gbps_needed_for_10_minutes": vol130 * 8 / 600 / 1e9,
    }
    print("E1 bandwidth (payload only):")
    for r in rows:
        print(f"  {r['channels']} ch x {r['fs_Hz']/1000:.0f} kHz x {r['bytes_per_sample']} B: "
              f"{r['GB_per_hour']:.0f} GB/h, {r['Mbit_per_s']:.0f} Mbit/s; "
              f"100 Mbps {r['hours_over_100Mbps']:.1f} h, 1 Gbps {r['minutes_over_1Gbps']:.0f} min, "
              f"10 Gbps {r['minutes_over_10Gbps']:.1f} min")
    print(f"  130 GB: {check['hours_at_100Mbps']:.1f} h at 100 Mbps, {check['minutes_at_1Gbps']:.0f} min at 1 Gbps; "
          f"10 min would need {check['Gbps_needed_for_10_minutes']:.2f} Gbit/s")
    return {"table": rows, "check_130GB": check}


def _naive_receiver(ls, n_bytes_total, busy_ms=0.0):
    """Reads with plain recv(65536) and records what each call returned.
    busy_ms: pause after every 10th call, as if the receiver were briefly busy."""
    import time
    conn, _ = ls.accept()
    sizes, starts_with_magic = [], []
    got = 0
    while got < n_bytes_total:
        b = conn.recv(65536)
        if not b:
            break
        sizes.append(len(b)); starts_with_magic.append(b[:4] == framing.MAGIC); got += len(b)
        if busy_ms and len(sizes) % 10 == 0:
            time.sleep(busy_ms / 1000)
    conn.close()
    return {"sizes": sizes, "starts_with_magic": starts_with_magic}


def _e2_run(cfg, busy_ms):
    frame_len = framing.HEADER.size + cfg.chunk_bytes
    ls = socket.socket(); ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ls.bind(("127.0.0.1", 0)); ls.listen(1)
    p, q = proc.start(_naive_receiver, ls, cfg.n_chunks * frame_len, busy_ms=busy_ms)
    tcp_sender("127.0.0.1", ls.getsockname()[1], cfg, pace=True)
    naive = proc.result(p, q); ls.close()
    sizes = np.array(naive["sizes"])
    return sizes, {"recv_calls": int(sizes.size),
                   "recv_returned_exactly_one_frame": int(np.sum(sizes == frame_len)),
                   "recv_calls_starting_at_a_frame_header": int(np.sum(naive["starts_with_magic"])),
                   "recv_size_median": float(np.median(sizes))}


def e2(out):
    cfg = StreamConfig(duration_s=1.0, chunk_ms=1.0)   # 40,840-byte frames: smaller than one recv(65536)
    frame_len = framing.HEADER.size + cfg.chunk_bytes
    res = {"frame_bytes": frame_len, "frames": cfg.n_chunks}
    fig, ax = plt.subplots(1, 2, figsize=(10, 3.4), sharey=False)
    for i, (busy, key, name) in enumerate(((0.0, "receiver_keeps_up", "receiver keeps up"),
                                            (20.0, "receiver_briefly_busy", "receiver busy 20 ms every 10 reads"))):
        sizes, r = _e2_run(cfg, busy)
        res[key] = r
        print(f"E2 {name}: {cfg.n_chunks} frames of {frame_len} B -> {r['recv_calls']} recv(65536) calls; "
              f"{r['recv_returned_exactly_one_frame']} returned exactly one frame, "
              f"{r['recv_calls_starting_at_a_frame_header']} began at a frame header")
        ax[i].hist(sizes / 1024, bins=40, color="0.35")
        ax[i].axvline(frame_len / 1024, color="tab:red", ls="--", label=f"one frame = {frame_len/1024:.1f} KiB")
        ax[i].set_xlabel("bytes returned by one recv(65536) (KiB)"); ax[i].set_ylabel("calls")
        ax[i].set_title(f"{name}: {r['recv_returned_exactly_one_frame']}/{r['recv_calls']} calls = one frame", fontsize=9)
        ax[i].legend(fontsize=8)
    fig.suptitle("TCP delivers a byte stream, not messages", fontsize=10)
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig_E2_recv_sizes.png"), dpi=160)
    return res


def e3(out):
    cfg = StreamConfig(duration_s=1.0)
    factors = [0.5, 1.0, 2.0, 4.0]
    rows = []
    for f in factors:
        t, _, _ = run_tcp(cfg, rate_factor=f)
        u, _, _ = run_udp(cfg, rate_factor=f)
        rows.append({"offered_MBps": cfg.bytes_per_second() / 1e6 * f,
                     "tcp_goodput_MBps": t["goodput_MBps"], "tcp_p99_ms": t["latency"]["p99_ms"],
                     "udp_goodput_MBps": u["goodput_MBps"], "udp_loss": u["loss_rate"],
                     "udp_chunks_complete": u["chunks_complete"], "udp_p99_ms": u["latency_complete_chunks"].get("p99_ms")})
        print(f"E3 offered {rows[-1]['offered_MBps']:.0f} MB/s: TCP {t['goodput_MBps']:.0f} MB/s p99 {t['latency']['p99_ms']:.1f} ms | "
              f"UDP {u['goodput_MBps']:.0f} MB/s loss {u['loss_rate']*100:.2f}% complete chunks {u['chunks_complete']}/{cfg.n_chunks}")
    fig, ax = plt.subplots(1, 2, figsize=(10, 3.6))
    x = [r["offered_MBps"] for r in rows]
    ax[0].plot(x, [r["tcp_goodput_MBps"] for r in rows], "o-", label="TCP")
    ax[0].plot(x, [r["udp_goodput_MBps"] for r in rows], "s-", label="UDP")
    ax[0].plot(x, x, "k:", lw=1, label="offered")
    ax[0].set_xlabel("offered load (MB/s)"); ax[0].set_ylabel("goodput (MB/s)"); ax[0].legend(); ax[0].set_title("Goodput")
    ax[1].plot(x, [r["udp_loss"] * 100 for r in rows], "s-", color="tab:orange")
    ax[1].set_xlabel("offered load (MB/s)"); ax[1].set_ylabel("UDP datagram loss (%)"); ax[1].set_title("UDP loss in this local experiment")
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig_E3_load_sweep.png"), dpi=160)
    return {"rows": rows, "note": "localhost; UDP loss is in the local receive path, not necessarily on the network; results depend on CPU and OS buffers"}


def e4(out):
    cfg = StreamConfig(duration_s=3.0)
    slow = 1.2 * cfg.chunk_ms                       # analysis takes 12 ms per 10-ms chunk
    kw = dict(rcvbuf=256 * 1024, sndbuf=256 * 1024)
    t0, tx0, rx0 = run_tcp(cfg, process_ms=0.0, **kw)          # baseline: receiver keeps up
    t, tx, rx = run_tcp(cfg, process_ms=slow, **kw)
    u, _, _ = run_udp(cfg, process_ms=slow, rcvbuf=256 * 1024)
    lat0 = (np.array(rx0["arrival_ns"]) - np.array(rx0["t_capture_ns"])) / 1e6
    lat = (np.array(rx["arrival_ns"]) - np.array(rx["t_capture_ns"])) / 1e6
    print(f"E4 slow receiver ({slow:.0f} ms per {cfg.chunk_ms:.0f}-ms chunk): TCP received {t['chunks_received']}/{cfg.n_chunks}, "
          f"latency grew to {lat.max():.0f} ms (baseline max {lat0.max():.0f} ms), max send block {t['max_send_block_ms']:.0f} ms | "
          f"UDP loss {u['loss_rate']*100:.1f}%, complete chunks {u['chunks_complete']}/{cfg.n_chunks}")
    q = lambda tx_: np.array([v if v is not None else np.nan for v in tx_["send_queue_bytes"]]) / 1024
    fig, ax = plt.subplots(1, 2, figsize=(10, 3.6))
    ax[0].plot(rx0["seq"], lat0, color="0.6", label="receiver keeps up")
    ax[0].plot(rx["seq"], lat, color="tab:blue", label="receiver 12 ms per chunk")
    ax[0].set_xlabel("chunk sequence number"); ax[0].set_ylabel("capture-to-arrival (ms)")
    ax[0].set_title("TCP: delivered frames, growing lag"); ax[0].legend(fontsize=8)
    ax[1].plot(q(tx0), color="0.6", label="receiver keeps up")
    ax[1].plot(q(tx), color="tab:green", label="receiver 12 ms per chunk")
    ax[1].set_xlabel("chunk sequence number"); ax[1].set_ylabel("sender kernel queue (KiB)")
    ax[1].set_title("Back-pressure: the send queue stays full"); ax[1].legend(fontsize=8)
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig_E4_backpressure.png"), dpi=160)
    return {"process_ms": slow, "tcp_baseline": t0, "tcp": t, "udp": u}


def e5(out):
    cfg = StreamConfig(channels=64, duration_s=2.0)  # fewer channels to keep payloads in memory
    u, _, rx = run_udp(cfg, process_ms=1.5 * cfg.chunk_ms, rcvbuf=64 * 1024, keep_payload=True)
    X, have = reconstruct(cfg, rx)
    mea = SyntheticMEA(cfg)
    X0 = np.concatenate([mea.chunk(k) for k in range(cfg.n_chunks)])
    thr = -5 * np.median(np.abs(X0), 0) / 0.6745
    def count(Y, H=None):
        below = (Y < thr)
        on = below[1:] & ~below[:-1]
        if H is not None:
            on &= H[1:] & H[:-1]
        return int(on.sum())
    n0, n1 = count(X0), count(X, have)
    missing = 1 - have.mean()
    print(f"E5 UDP with slow receiver: {missing*100:.1f}% of samples missing; spikes detected {n1} of {n0}")
    c = 0; seg = slice(0, int(0.2 * cfg.fs))
    fig, ax = plt.subplots(figsize=(9, 3))
    t = np.arange(X0.shape[0])[seg] / cfg.fs * 1000
    ax.plot(t, X0[seg, c] * 0.2, color="0.6", lw=0.8, label="sent")
    y = np.where(have[seg, c], X[seg, c] * 0.2, np.nan)
    ax.plot(t, y, color="tab:blue", lw=0.8, label="received")
    ax.fill_between(t, ax.get_ylim()[0], ax.get_ylim()[1], where=~have[seg, c], color="tab:red", alpha=0.15, label="missing")
    ax.set_xlabel("time (ms)"); ax.set_ylabel("µV (synthetic)"); ax.legend(loc="lower right", fontsize=8)
    ax.set_title("UDP loss removes stretches of the waveform")
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig_E5_waveform.png"), dpi=160)
    return {"udp": u, "missing_sample_fraction": float(missing), "spikes_sent": n0, "spikes_detected_after_loss": n1}


EXPS = {"E1": e1, "E2": e2, "E3": e3, "E4": e4, "E5": e5}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("which", nargs="?", default="all")
    ap.add_argument("--out", default="results")
    a = ap.parse_args(argv)
    os.makedirs(a.out, exist_ok=True)
    names = list(EXPS) if a.which == "all" else [a.which]
    res = {"environment": {"python": sys.version.split()[0], "platform": sys.platform, "host": "127.0.0.1 (loopback)"}}
    for n in names:
        res[n] = EXPS[n](a.out)
    res["environment"].update(metrics.process_usage())
    with open(os.path.join(a.out, "summary.json" if a.which == "all" else f"{a.which}.json"), "w") as f:
        json.dump(res, f, indent=2, default=float)


if __name__ == "__main__":
    main()
