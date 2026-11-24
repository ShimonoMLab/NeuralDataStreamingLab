import socket, threading, time, zlib
import numpy as np
import pytest
from nslab.config import StreamConfig, required_bandwidth, transfer_time
from nslab.signal import SyntheticMEA
from nslab import framing
from nslab.tcp_stream import run_tcp, tcp_sender
from nslab.udp_stream import run_udp


def test_bandwidth_arithmetic():
    vol, bit = required_bandwidth(1020, 30_000, 2, 3600)
    assert vol == pytest.approx(220.32e9)          # 1,020 ch x 30 kHz x int16 x 1 h
    assert bit == pytest.approx(489.6e6)
    assert required_bandwidth(1020, 20_000, 2, 3600)[0] == pytest.approx(146.88e9)
    assert 130e9 * 8 / 600 / 1e9 == pytest.approx(1.733, abs=1e-3)   # 130 GB in 10 min needs ~1.73 Gbit/s
    assert transfer_time(130e9, 100e6, efficiency=1.0) / 3600 == pytest.approx(2.89, abs=0.01)


def test_frame_roundtrip_and_crc():
    payload = bytes(range(256)) * 10
    f = framing.pack(7, 123, 4, 5, payload, last=True)
    h = framing.unpack_header(f[:framing.HEADER.size])
    assert (h["seq"], h["t_capture_ns"], h["n_ch"], h["n_samp"], h["last"]) == (7, 123, 4, 5, True)
    assert h["payload_len"] == len(payload) and h["crc32"] == zlib.crc32(payload)
    bad = bytearray(f); bad[0] ^= 0xFF
    with pytest.raises(ValueError):
        framing.unpack_header(bytes(bad[:framing.HEADER.size]))


def test_read_frame_survives_partial_writes():
    a, b = socket.socketpair()
    payload = np.arange(5000, dtype=np.int16).tobytes()
    frame = framing.pack(1, 0, 10, 500, payload)
    def trickle():
        for i in range(0, len(frame), 777):          # deliberately odd piece size
            a.sendall(frame[i:i + 777]); time.sleep(0.001)
    th = threading.Thread(target=trickle); th.start()
    sizes = []
    h, got = framing.read_frame(b, sizes)
    th.join(); a.close(); b.close()
    assert got == payload and h["crc_ok"] and len(sizes) > 2


def test_signal_is_deterministic():
    cfg = StreamConfig(channels=16, duration_s=0.05)
    x1, x2 = SyntheticMEA(cfg).chunk(3), SyntheticMEA(cfg).chunk(3)
    assert x1.dtype == np.int16 and x1.shape == (cfg.samples_per_chunk, 16)
    assert np.array_equal(x1, x2)


def test_tcp_end_to_end_integrity():
    cfg = StreamConfig(channels=64, duration_s=0.3)
    s, _, rx = run_tcp(cfg, keep_payload=True)
    assert s["chunks_received"] == cfg.n_chunks and s["in_order"] and s["crc_all_ok"]
    mea = SyntheticMEA(cfg)
    for k, p in rx["payloads"].items():
        assert p == mea.chunk(k).tobytes()


def test_udp_counts_simulated_loss():
    cfg = StreamConfig(channels=64, duration_s=0.3)
    s, tx, _ = run_udp(cfg, rate_factor=0.5, rcvbuf=4 * 1024 * 1024, drop_prob=0.05)
    expected = s["datagrams_expected"]
    assert tx["skipped"] > 0
    assert s["datagrams_received"] <= expected - tx["skipped"]
    assert s["datagrams_received"] >= 0.95 * (expected - tx["skipped"])


def test_non_loopback_refused():
    with pytest.raises(PermissionError):
        tcp_sender("192.0.2.1", 9, StreamConfig(duration_s=0.01))   # TEST-NET-1, never contacted


def test_udp_partial_fragment_goodput_and_reconstruction():
    """The final fragment's short length must not count as a full 1200 B."""
    from nslab.udp_stream import reconstruct
    cfg = StreamConfig(channels=3, fs=1000, chunk_ms=7.0, duration_s=0.028)
    report, _, received = run_udp(cfg, rate_factor=0.5, max_payload=16,
                                  rcvbuf=1024 * 1024, keep_payload=True)
    assert report["datagrams_received"] == report["datagrams_expected"]
    assert received["payload_bytes_received"] == cfg.chunk_bytes * cfg.n_chunks
    assert report["crc_failures"] == 0 and report["invalid_datagrams"] == 0
    x, have = reconstruct(cfg, received, max_payload=16)
    truth = np.concatenate([SyntheticMEA(cfg).chunk(k) for k in range(cfg.n_chunks)])
    assert have.all()
    assert np.array_equal(x, truth)


def test_udp_rejects_corrupted_payload_crc():
    """Invalid UDP payloads must not enter waveform reconstruction."""
    from nslab.udp_stream import udp_receiver
    cfg = StreamConfig(channels=2, fs=1000, chunk_ms=1, duration_s=0.001)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    src = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target = sock.getsockname()
    payload = SyntheticMEA(cfg).chunk(0).tobytes()
    packet = framing.pack(0, 0, 2, 1, payload)
    corrupted = bytearray(packet)
    corrupted[-1] ^= 0x01
    out = {}
    thread = threading.Thread(target=lambda: out.update(udp_receiver(sock, cfg, idle_timeout=0.5)))
    thread.start()
    src.sendto(corrupted, target)
    src.sendto(packet, target)
    src.sendto(framing.pack(1, 0, 0, 0, b"", last=True), target)
    thread.join(timeout=2)
    assert not thread.is_alive()
    sock.close(); src.close()
    assert out["crc_failures"] == 1
    assert out["payload_bytes_received"] == len(payload)
    assert len(out["got"][0]) == 1


def test_e6_deadline_small():
    from nslab.deadline import run
    import numpy as np
    lat, ovh = run("arq", 0.10, 1, n=300, seed=5, wait_ms=500)
    assert np.mean(lat <= 450) > 0.99 and 1.0 < ovh < 1.4
    lat0, ovh0 = run("none", 0.10, 1, n=300, seed=5, wait_ms=200)
    assert 0.8 < np.mean(np.isfinite(lat0)) < 0.97 and abs(ovh0 - 1.0) < 1e-9
