"""E6: loss, retransmission and deadlines (localhost, emulated lossy link).

A sender emits one small UDP packet every 5 ms with a sequence number.
Packets pass through a relay that emulates a lossy link: a fixed one-way
delay and packet drops, either independent or in bursts (Gilbert-Elliott).
Three ways of handling loss are compared:

  none   the receiver only learns from the sequence numbers which packets
         are missing (a loss mask), as a fast process that cannot wait must;
  fec    one XOR parity packet after every k data packets (recovers one
         loss per group, costs 1/k extra bandwidth);
  arq    the receiver sends a NACK for missing sequence numbers and repeats
         it after a timeout; the sender retransmits (selective repeat, as
         TCP's SACK-based recovery does in spirit, without congestion control).

For every packet the time from its first transmission to its delivery (or
recovery) is recorded, so one run gives the delivered fraction for every
deadline D.  The question is the one TCP and UDP answer differently: how
long a process can wait decides which mechanism pays off.

The drops are emulated in the application relay on 127.0.0.1; they are not
measurements of an external network.

  python -m nslab.deadline            # writes results/E6_deadline.json and a figure
"""
import heapq, json, os, select, socket, struct, threading, time
import numpy as np

HDR = struct.Struct("!BIq")          # kind, seq, send time (ns)
DATA, PARITY, NACK = 0, 1, 2
PAYLOAD = 200


def ge_drops(n, p, burst, rng):
    """Boolean drop decisions for n packets: independent if burst <= 1,
    otherwise Gilbert-Elliott with mean burst length `burst` and loss rate p."""
    if p <= 0: return np.zeros(n, bool)
    if burst <= 1: return rng.random(n) < p
    q_exit = 1.0 / burst; q_enter = p * q_exit / (1 - p)
    out = np.zeros(n, bool); bad = False; u = rng.random(n)
    for k in range(n):
        bad = (u[k] >= q_exit) if bad else (u[k] < q_enter); out[k] = bad
    return out


class Relay(threading.Thread):
    """Forwards sender->receiver with drops and delay, receiver->sender with
    independent drops at the same rate and the same delay."""
    def __init__(self, delay_ms, p, burst, seed, n_max=200000):
        super().__init__(daemon=True)
        self.a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); self.a.bind(("127.0.0.1", 0))
        self.b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); self.b.bind(("127.0.0.1", 0))
        self.delay = delay_ms * 1e6; rng = np.random.default_rng(seed)
        self.fwd = ge_drops(n_max, p, burst, rng); self.rev = rng.random(n_max) < p
        self.nf = self.nr = 0; self.dst = None; self.src = None; self.stop = threading.Event()
        self.heap = []; self.k = 0; self.dropped = 0; self.forwarded = 0

    def run(self):
        while not self.stop.is_set():
            now = time.perf_counter_ns()
            while self.heap and self.heap[0][0] <= now:
                _, _, sock, addr, msg = heapq.heappop(self.heap); sock.sendto(msg, addr)
            timeout = 0.002 if not self.heap else max(0.0, (self.heap[0][0] - now) / 1e9)
            r, _, _ = select.select([self.a, self.b], [], [], min(timeout, 0.002))
            for s in r:
                msg, addr = s.recvfrom(2048)
                if s is self.a:
                    self.src = addr; drop = self.fwd[self.nf % self.fwd.size]; self.nf += 1
                    out, to = self.b, self.dst
                else:
                    drop = self.rev[self.nr % self.rev.size]; self.nr += 1; out, to = self.a, self.src
                if drop or to is None: self.dropped += 1; continue
                self.forwarded += 1; self.k += 1
                heapq.heappush(self.heap, (time.perf_counter_ns() + self.delay, self.k, out, to, msg))


def run(scheme, p, burst, n=1000, interval_ms=5.0, delay_ms=20.0, k=4, seed=1, wait_ms=600.0,
        nack_timeout_ms=None):
    relay = Relay(delay_ms, p, burst, seed); relay.start()
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); rx.bind(("127.0.0.1", 0))
    relay.dst = rx.getsockname()
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); tx.bind(("127.0.0.1", 0))
    to_relay = relay.a.getsockname()
    sent_at = np.zeros(n, np.int64); got_at = np.full(n, -1, np.int64)
    payload = {}; bytes_sent = [0]; done = threading.Event()
    rto = (nack_timeout_ms or (2 * delay_ms + 10)) * 1e6
    body = lambda s: (s.to_bytes(4, "big") * (PAYLOAD // 4))

    def receiver():
        highest = -1; nacked = {}; groups = {}
        while not done.is_set():
            r, _, _ = select.select([rx], [], [], 0.002)
            now = time.perf_counter_ns()
            if r:
                msg, _ = rx.recvfrom(4096); kind, seq, t0 = HDR.unpack_from(msg)
                if kind == DATA:
                    if got_at[seq] < 0: got_at[seq] = now
                    if seq > highest:
                        for m in range(highest + 1, seq): nacked.setdefault(m, 0)   # gap seen
                        highest = seq
                    groups.setdefault(seq // k, {})[seq] = msg[HDR.size:]
                elif kind == PARITY:
                    groups.setdefault(seq, {})["P"] = msg[HDR.size:]
                    for m in range(highest + 1, min(n, (seq + 1) * k)): nacked.setdefault(m, 0)
                    highest = max(highest, min(n, (seq + 1) * k) - 1)
                if scheme == "fec":                                   # one missing packet per group
                    g = seq // k if kind == DATA else seq; G = groups[g]
                    members = [m for m in range(g * k, min(n, g * k + k))]
                    miss = [m for m in members if got_at[m] < 0]
                    if "P" in G and len(miss) == 1 and all(m in G for m in members if m != miss[0]):
                        got_at[miss[0]] = now
            if scheme == "arq":
                due = [m for m, t in nacked.items() if got_at[m] < 0 and now - t >= rto]
                if due:
                    for m in due: nacked[m] = now
                    for i in range(0, len(due), 64):
                        part = due[i:i + 64]
                        rx.sendto(HDR.pack(NACK, len(part), 0) + struct.pack(f"!{len(part)}I", *part), relay.b.getsockname())

    def sender_nacks():
        while not done.is_set():
            r, _, _ = select.select([tx], [], [], 0.002)
            if not r: continue
            msg, _ = tx.recvfrom(4096); kind, cnt, _ = HDR.unpack_from(msg)
            if kind != NACK: continue
            for m in struct.unpack_from(f"!{cnt}I", msg, HDR.size):
                pkt = HDR.pack(DATA, m, int(sent_at[m])) + payload[m]
                tx.sendto(pkt, to_relay); bytes_sent[0] += len(pkt)

    th = [threading.Thread(target=receiver, daemon=True)]
    if scheme == "arq": th.append(threading.Thread(target=sender_nacks, daemon=True))
    for t in th: t.start()
    t_start = time.perf_counter_ns(); parity = bytearray(PAYLOAD)
    for s in range(n):
        wait = (t_start + s * interval_ms * 1e6 - time.perf_counter_ns()) / 1e9
        if wait > 0: time.sleep(wait)           # sleep, so the relay and receiver threads get the CPU
        payload[s] = body(s); sent_at[s] = time.perf_counter_ns()
        pkt = HDR.pack(DATA, s, int(sent_at[s])) + payload[s]
        tx.sendto(pkt, to_relay); bytes_sent[0] += len(pkt)
        if scheme == "fec":
            parity = bytearray(a ^ b for a, b in zip(parity, payload[s]))
            if s % k == k - 1 or s == n - 1:
                pp = HDR.pack(PARITY, s // k, 0) + bytes(parity); tx.sendto(pp, to_relay)
                bytes_sent[0] += len(pp); parity = bytearray(PAYLOAD)
    time.sleep(wait_ms / 1000); done.set()
    for t in th: t.join(1)
    relay.stop.set(); relay.join(1)
    lat = np.where(got_at >= 0, (got_at - sent_at) / 1e6, np.inf)
    data_bytes = n * (HDR.size + PAYLOAD)
    return lat, bytes_sent[0] / data_bytes


def main(out="results"):
    os.makedirs(out, exist_ok=True)
    deadlines = [25, 50, 75, 100, 150, 200, 400]
    res = {"setup": {"packets": 1000, "interval_ms": 5.0, "one_way_delay_ms": 20.0, "loss_rate": 0.10,
                     "fec_group": 4, "note": "drops emulated in an application relay on 127.0.0.1"},
           "runs": []}
    for loss, burst in (("independent", 1), ("bursts (mean 10 packets)", 10)):
        for scheme in ("none", "fec", "arq"):
            lat, ovh = run(scheme, 0.10, burst, seed=3)
            row = {"loss": loss, "scheme": scheme, "bandwidth_ratio": round(ovh, 3),
                   "delivered_by_deadline": {d: round(float(np.mean(lat <= d)), 3) for d in deadlines},
                   "median_ms_delivered": round(float(np.median(lat[np.isfinite(lat)])), 1)}
            res["runs"].append(row)
            print(f"E6 {loss:26s} {scheme:4s} bw x{ovh:.2f}  " +
                  "  ".join(f"{d}ms:{v:.3f}" for d, v in row["delivered_by_deadline"].items()))
    with open(os.path.join(out, "E6_deadline.json"), "w") as f: json.dump(res, f, indent=1)
    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
        fig, axs = plt.subplots(1, 2, figsize=(9, 3.4), sharey=True)
        for ax, loss in zip(axs, ("independent", "bursts (mean 10 packets)")):
            for r in res["runs"]:
                if r["loss"] != loss: continue
                ax.plot(deadlines, list(r["delivered_by_deadline"].values()), marker="o",
                        label=f"{r['scheme']} (bandwidth x{r['bandwidth_ratio']:.2f})")
            ax.set_xscale("log"); ax.set_title(f"10% loss, {loss}"); ax.set_xlabel("deadline D (ms)")
            ax.grid(alpha=.3); ax.legend(fontsize=8)
        axs[0].set_ylabel("delivered by D")
        fig.tight_layout(); fig.savefig(os.path.join(out, "fig_E6_deadline.png"), dpi=130)
    except Exception as e:
        print("figure skipped:", e)
    return res


if __name__ == "__main__":
    main()
