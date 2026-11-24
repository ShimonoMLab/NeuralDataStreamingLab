"""Deterministic synthetic multi-channel neural voltages.

Background noise + a slow common oscillation + sparse spikes, scaled to
int16. Chunk k is a pure function of (config, k), so a receiver can
regenerate any chunk to check what arrived.
"""
import numpy as np
from .config import StreamConfig


class SyntheticMEA:
    def __init__(self, cfg: StreamConfig, bank_seconds: float = 0.5):
        self.cfg = cfg
        rng = np.random.default_rng(cfg.seed)
        nb = int(cfg.fs * bank_seconds)
        self.bank = (rng.standard_normal((nb, cfg.channels)) * 8.0).astype(np.float32)   # ~8 uV noise
        self.phase = rng.uniform(0, 2 * np.pi, cfg.channels).astype(np.float32)
        self.rate = rng.uniform(0.5, 8.0, cfg.channels)                                     # spikes/s
        t = np.arange(int(cfg.fs * 0.0015)) / cfg.fs
        self.template = (-80 * np.exp(-((t - 0.0004) / 0.00015) ** 2)
                         + 30 * np.exp(-((t - 0.0009) / 0.0003) ** 2)).astype(np.float32)   # biphasic spike (uV)
        self.uv_per_lsb = 0.2

    def chunk(self, k: int) -> np.ndarray:
        """Return chunk k as (samples, channels) int16."""
        cfg = self.cfg
        n = cfg.samples_per_chunk
        start = k * n
        idx = (start + np.arange(n)) % self.bank.shape[0]
        x = self.bank[idx].copy()
        t = (start + np.arange(n)) / cfg.fs
        x += 20.0 * np.sin(2 * np.pi * 6.0 * t[:, None] + self.phase[None, :])           # 6 Hz common rhythm
        rng = np.random.default_rng((cfg.seed, k))
        p = self.rate * n / cfg.fs
        for c in np.nonzero(rng.random(cfg.channels) < p)[0]:
            s = int(rng.integers(0, n))
            e = min(n, s + self.template.size)          # a spike near the chunk end is truncated
            x[s:e, c] += self.template[:e - s]
        return np.clip(np.round(x / self.uv_per_lsb), -32768, 32767).astype(cfg.dtype)
