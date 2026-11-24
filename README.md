# NeuralDataStreamingLab

Synthetic multi-electrode neural voltages are streamed over **TCP and UDP on
localhost** to teach framing, bandwidth, buffering, flow control, loss, and retransmission against deadlines.

神経活動の**合成波形**を使い、TCP/IP と UDP の違いを「動かして測る」授業教材です。
**実計測器との接続やリアルタイム性の保証はありません。**

## Quick start / 実行方法

Linux / Python 3.10+ を対象とします。今回の修正版は Linux・Python 3.13 で検証しました。

```bash
python -m pip install -r requirements.txt
python -m pytest -q tests              # 10 tests
python -m nslab.experiments all        # E1–E5; generates results/
python -m nslab.experiments E4         # run one experiment
python -m nslab.deadline               # E6: loss, retransmission and deadlines
```

- 学生用: [`docs/worksheet_ja.md`](docs/worksheet_ja.md)
- 教員用: [`docs/teacher_notes_ja.md`](docs/teacher_notes_ja.md)
- 15回の授業計画例（講義・測定演習・到達の目安・評価・教職課程の追加課題）: [`docs/course_plan_ja.md`](docs/course_plan_ja.md)
- `results/`: **one example run**, not universal expected values. Re-run on
  your own machine and report its OS, Python version, and conditions.

## Experiments / 実験

| | Main question | What is measured |
|---|---|---|
| E1 | How much bandwidth is required? | Channel count × sampling frequency × sample storage size |
| E2 | Is TCP a message protocol? | Sizes returned by `recv()` and application-level frame handling |
| E3 | What happens as the sender increases its data rate? | TCP/UDP application payload goodput, UDP fragment loss |
| E4 | What happens when the analysis is slower than acquisition? | TCP back-pressure and growing lag, UDP loss |
| E5 | What does missing data do to the waveform? | Missing sample mask and elementary spike detection |
| E6 | How long can a process wait, and what does that buy? | Delivered fraction by deadline with no repair (loss mask only), XOR parity, and NACK retransmission, under independent and bursty loss on an emulated lossy link |

### Assumptions / 実測条件との区別

The default synthetic data format is **1,020 channels × 20 kHz × 16-bit samples**
(40.8 MB/s = 146.88 GB/h of sample payload). The sampling frequency and sample
storage format are **educational assumptions**, not confirmed operating
settings of Shimono's MEA equipment. The 2020 research report says that
**1,020 sensors were selected from 26,000 electrodes** and that a recorded
hour produced **approximately 130 GB**. It does **not** specify sampling
frequency or bit depth. The data volumes must not be presented as identical.

E1 also illustrates 30 kHz/16 bit as an alternative assumption, and
20 kHz/10 bit as a *theoretical tightly packed* example. Real sample storage
may use padding, compression, or metadata. Frame/IP/Ethernet overhead is not
included in raw sample throughput.

The report describes finding a **100 Mbps bottleneck** and reducing transfer
time from over an hour to around ten minutes. Transferring 130 GB in ten
minutes requires at least **1.73 Gbit/s of average application payload rate**,
which cannot be explained by one 1 Gbps link alone. The original data volume,
link configuration and actual transferred bytes would need verification;
this is a useful classroom consistency check, not a claim about the hardware.

### What the measurements mean / 測定の限界

- `results/summary.json` and the PNG figures are snapshots of one Linux
  **loopback** run. CPU scheduling, socket buffers and host load substantially
  change the measured results.
- UDP `goodput_MBps` counts **actual accepted payload bytes**, not the maximum
  fragment length. Corrupt UDP datagrams are rejected by the educational
  application's CRC-32 check (not provided by UDP itself).
- UDP datagram loss in these experiments happens in the **local send/receive
  path**; it does not demonstrate packet loss on an external network.
- TCP preserves ordered bytes during a successful connection but makes no
  promise of low latency or timing deadlines. The simulated capture timestamp
  follows an **ideal acquisition schedule**; measured lag is from that
  schedule to receiving a frame on the **same host clock**, not a real device's
  capture-to-analysis latency.
- `tcp_stream.py` and `udp_stream.py` contain low-level sender functions, but
  the `nslab.experiments` runner binds and connects **only to 127.0.0.1**.
  Two-host LAN experiments are **not implemented or validated** in this release.
- E6 drops packets in an **application relay on 127.0.0.1** (fixed 20-ms
  one-way delay, 10 % loss); it teaches sequence numbers, NACK/timeout
  retransmission and parity, but it is not TCP's own retransmission and has no
  congestion control. One example run is in `results/E6_deadline.json`.
- This version does **not** inspect TCP congestion window (`cwnd`), the kernel's TCP
  retransmissions, Reno/CUBIC algorithm choice, or IP routing. Those are
  possible later lessons, **not demonstrated results**.
- Waveforms are **entirely synthetic**; no patient, animal, or personal
  recordings are distributed.

## Source / 実績の出典

M. Shimono, 「多神経細胞間での情報流ネットワークの制御」,
上原記念生命科学財団研究報告集 34 (2020).

Protocol references: RFC 9293 (TCP), RFC 768 (UDP), RFC 8085 (UDP usage),
RFC 9438 (CUBIC, not implemented).

## License

MIT (`LICENSE`).
