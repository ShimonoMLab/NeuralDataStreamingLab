"""Binary framing for a byte stream.

TCP delivers a stream of bytes, not messages: one recv() may return part
of a frame, or the end of one frame and the start of the next. The
receiver therefore needs the frame length up front and must read exactly
that many bytes. This module fixes a header layout and provides
recv_exact() for that purpose.

Header (network byte order, 40 bytes)
  magic        4s   b"NSL1"
  version      B
  flags        B    bit0: last frame
  n_frag       H    UDP: number of fragments in this chunk (TCP: 1)
  seq          Q    chunk sequence number
  t_capture_ns q    sender clock when the chunk was "captured"
  frag         H    UDP: fragment index (TCP: 0)
  n_ch         H    channels
  n_samp       I    samples per channel in the chunk
  payload_len  I    bytes following the header
  crc32        I    CRC-32 of the payload
"""
import struct, zlib, socket

HEADER = struct.Struct("!4sBBHQqHHIII")
MAGIC = b"NSL1"
VERSION = 1
FLAG_LAST = 1
assert HEADER.size == 40


def pack(seq, t_capture_ns, n_ch, n_samp, payload: bytes, last=False, frag=0, n_frag=1):
    return HEADER.pack(MAGIC, VERSION, FLAG_LAST if last else 0, n_frag, seq, t_capture_ns,
                       frag, n_ch, n_samp, len(payload), zlib.crc32(payload)) + payload


def unpack_header(b: bytes):
    magic, ver, flags, n_frag, seq, t_ns, frag, n_ch, n_samp, plen, crc = HEADER.unpack(b)
    if magic != MAGIC or ver != VERSION:
        raise ValueError("not an NSL1 frame (lost frame alignment?)")
    return {"last": bool(flags & FLAG_LAST), "n_frag": n_frag, "seq": seq, "t_capture_ns": t_ns,
            "frag": frag, "n_ch": n_ch, "n_samp": n_samp, "payload_len": plen, "crc32": crc}


def recv_exact(sock: socket.socket, n: int, sizes=None) -> bytes:
    """Read exactly n bytes from a stream socket. `sizes`, if a list,
    records how many bytes each recv() returned (for teaching)."""
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        k = sock.recv_into(view[got:], n - got)
        if k == 0:
            raise ConnectionError(f"peer closed after {got} of {n} bytes")
        if sizes is not None:
            sizes.append(k)
        got += k
    return bytes(buf)


def read_frame(sock, sizes=None):
    h = unpack_header(recv_exact(sock, HEADER.size, sizes))
    payload = recv_exact(sock, h["payload_len"], sizes)
    h["crc_ok"] = (zlib.crc32(payload) == h["crc32"])
    return h, payload
