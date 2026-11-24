"""NeuralDataStreamingLab: teaching TCP/IP and UDP with synthetic
multi-electrode neural data.

Modules
  config      stream parameters and bandwidth arithmetic
  signal      deterministic synthetic neural voltages (int16)
  framing     binary frame header, CRC, exact reads from a byte stream
  tcp_stream  TCP sender/receiver with real-time pacing and metrics
  udp_stream  UDP sender/receiver with sequence numbers and rate control
  metrics     throughput, latency percentiles, jitter, socket queues
  experiments five classroom experiments on localhost
"""
__version__ = "0.1.0"
