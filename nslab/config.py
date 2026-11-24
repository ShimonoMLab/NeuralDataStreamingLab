"""Stream parameters and bandwidth arithmetic.

All defaults are EDUCATIONAL ASSUMPTIONS, not the settings of any
specific recording system. Replace them with the real channel count,
sampling rate and sample format once these are known.
"""
from dataclasses import dataclass
import numpy as np


@dataclass
class StreamConfig:
    channels: int = 1020          # selected readout channels (assumption)
    fs: int = 20_000              # samples per second per channel (assumption)
    dtype: str = "int16"          # stored sample format (assumption)
    chunk_ms: float = 10.0        # one frame carries this much time
    duration_s: float = 2.0       # length of a run
    seed: int = 0

    @property
    def bytes_per_sample(self) -> int:
        return np.dtype(self.dtype).itemsize

    @property
    def samples_per_chunk(self) -> int:
        return int(round(self.fs * self.chunk_ms / 1000))

    @property
    def chunk_bytes(self) -> int:
        return self.channels * self.samples_per_chunk * self.bytes_per_sample

    @property
    def n_chunks(self) -> int:
        return int(round(self.duration_s * 1000 / self.chunk_ms))

    def bytes_per_second(self) -> float:
        """data volume = channels x fs x bytes/sample (payload only)"""
        return self.channels * self.fs * self.bytes_per_sample


def required_bandwidth(channels, fs, bytes_per_sample, seconds=3600):
    """Return payload bytes for `seconds`, and the bit rate in bit/s.
    Headers (frame, TCP/IP, Ethernet) are not included."""
    bps = channels * fs * bytes_per_sample
    return bps * seconds, bps * 8


def transfer_time(n_bytes, link_bps, efficiency=0.94):
    """Seconds to move n_bytes over a link of link_bps at the given
    assumed goodput efficiency (0.94 is an illustrative classroom
    value, not a guaranteed TCP/IP or Ethernet efficiency)."""
    return n_bytes * 8 / (link_bps * efficiency)


def bandwidth_table():
    rows = []
    for ch, fs, b in ((1020, 20_000, 2), (1020, 30_000, 2), (1024, 20_000, 1.25)):
        vol, bit = required_bandwidth(ch, fs, b)
        rows.append({
            "channels": ch, "fs_Hz": fs, "bytes_per_sample": b,
            "GB_per_hour": vol / 1e9, "Mbit_per_s": bit / 1e6,
            "hours_over_100Mbps": transfer_time(vol, 100e6) / 3600,
            "minutes_over_1Gbps": transfer_time(vol, 1e9) / 60,
            "minutes_over_10Gbps": transfer_time(vol, 10e9) / 60,
        })
    return rows
