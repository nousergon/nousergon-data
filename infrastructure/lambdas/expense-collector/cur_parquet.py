"""Dependency-free reader for the AWS billing export's parquet files.

The expense collector reads AWS spend from the CUR 2.0 billing export instead
of Cost Explorer (alpha-engine-config-I12168: zero `ce:` calls, permanently).
The export is parquet. pyarrow is ~43 MB compressed, which on top of this
Lambda's existing dependencies does not fit the 50 MB direct-upload zip
`deploy.sh` ships. So this module decodes the small subset of parquet that the
export actually writes, in the standard library alone.

**What it supports**, and every combination is exercised by
``test_cur_parquet.py`` against files pyarrow wrote:

  * flat schemas (no nested or repeated columns), REQUIRED or OPTIONAL;
  * data page v1 and v2, dictionary pages;
  * PLAIN, PLAIN_DICTIONARY and RLE_DICTIONARY value encodings, RLE levels;
  * UNCOMPRESSED, SNAPPY and GZIP chunks;
  * INT32, INT64, INT96, DOUBLE, FLOAT and BYTE_ARRAY physical types, with
    INT96 and timestamp-annotated INT64 returned as UTC datetimes and UTF-8
    annotated BYTE_ARRAY returned as str.

**Anything else RAISES** :class:`ParquetUnsupported`. The caller records that
on the AWS row as an error. A decoder that guessed would turn an
unrecognised file into a wrong dollar figure, which is worse than no figure.

Measured 2026-10-08 against the live export (parquet-mr 1.13.1 via Spark
3.5.6): data page v1, PLAIN_DICTIONARY with a PLAIN fallback column, RLE
levels, SNAPPY, INT96 timestamps. Every value matched pyarrow on both the
2026-09 and 2026-10 periods.
"""

from __future__ import annotations

import struct
import zlib
from datetime import datetime, timedelta, timezone


class ParquetUnsupported(ValueError):
    """The file uses a parquet feature this reader does not decode."""


# ---------------------------------------------------------------------------
# Thrift compact protocol (the parquet footer and page headers)
# ---------------------------------------------------------------------------

_T_STOP, _T_TRUE, _T_FALSE, _T_BYTE, _T_I16, _T_I32, _T_I64 = 0, 1, 2, 3, 4, 5, 6
_T_DOUBLE, _T_BINARY, _T_LIST, _T_SET, _T_MAP, _T_STRUCT = 7, 8, 9, 10, 11, 12


class _Thrift:
    """Generic compact-protocol reader: a struct becomes ``{field_id: value}``."""

    def __init__(self, buf: bytes, pos: int = 0) -> None:
        self.buf = buf
        self.pos = pos

    def _byte(self) -> int:
        b = self.buf[self.pos]
        self.pos += 1
        return b

    def varint(self) -> int:
        shift = result = 0
        while True:
            b = self._byte()
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return result
            shift += 7

    def zigzag(self) -> int:
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def _value(self, ttype: int):
        if ttype == _T_TRUE:
            return True
        if ttype == _T_FALSE:
            return False
        if ttype == _T_BYTE:
            return struct.unpack("b", bytes([self._byte()]))[0]
        if ttype in (_T_I16, _T_I32, _T_I64):
            return self.zigzag()
        if ttype == _T_DOUBLE:
            v = struct.unpack_from("<d", self.buf, self.pos)[0]
            self.pos += 8
            return v
        if ttype == _T_BINARY:
            n = self.varint()
            v = self.buf[self.pos:self.pos + n]
            self.pos += n
            return v
        if ttype in (_T_LIST, _T_SET):
            head = self._byte()
            size, etype = head >> 4, head & 0x0F
            if size == 15:
                size = self.varint()
            if etype in (_T_TRUE, _T_FALSE):  # bools in a list are one byte each
                return [self._byte() == 1 for _ in range(size)]
            return [self._value(etype) for _ in range(size)]
        if ttype == _T_MAP:
            size = self.varint()
            if size == 0:
                return {}
            kv = self._byte()
            return {self._value(kv >> 4): self._value(kv & 0x0F) for _ in range(size)}
        if ttype == _T_STRUCT:
            return self.struct()
        raise ParquetUnsupported(f"thrift type {ttype}")

    def struct(self) -> dict:
        out: dict = {}
        last = 0
        while True:
            head = self._byte()
            ttype = head & 0x0F
            if ttype == _T_STOP:
                return out
            delta = head >> 4
            fid = last + delta if delta else self.zigzag()
            out[fid] = self._value(ttype)
            last = fid


# ---------------------------------------------------------------------------
# Snappy (raw block format, as parquet stores it)
# ---------------------------------------------------------------------------

def snappy_decompress(data: bytes) -> bytes:
    src = _Thrift(data)
    length = src.varint()
    out = bytearray()
    pos = src.pos
    n = len(data)
    while pos < n:
        tag = data[pos]
        pos += 1
        kind = tag & 3
        if kind == 0:  # literal
            size = tag >> 2
            if size >= 60:
                extra = size - 59
                size = int.from_bytes(data[pos:pos + extra], "little")
                pos += extra
            size += 1
            out += data[pos:pos + size]
            pos += size
            continue
        if kind == 1:
            size = ((tag >> 2) & 7) + 4
            offset = ((tag >> 5) << 8) | data[pos]
            pos += 1
        elif kind == 2:
            size = (tag >> 2) + 1
            offset = int.from_bytes(data[pos:pos + 2], "little")
            pos += 2
        else:
            size = (tag >> 2) + 1
            offset = int.from_bytes(data[pos:pos + 4], "little")
            pos += 4
        if offset == 0 or offset > len(out):
            raise ParquetUnsupported("corrupt snappy stream (bad copy offset)")
        start = len(out) - offset
        if offset >= size:
            out += out[start:start + size]
        else:  # overlapping copy repeats the window
            for i in range(size):
                out.append(out[start + i])
    if len(out) != length:
        raise ParquetUnsupported(f"snappy length {len(out)} != declared {length}")
    return bytes(out)


_CODECS = {0: "UNCOMPRESSED", 1: "SNAPPY", 2: "GZIP"}


def _decompress(codec: int, data: bytes) -> bytes:
    if codec == 0:
        return data
    if codec == 1:
        return snappy_decompress(data)
    if codec == 2:
        return zlib.decompress(data, 16 + zlib.MAX_WBITS)
    raise ParquetUnsupported(f"compression codec {codec}")


# ---------------------------------------------------------------------------
# RLE / bit-packing hybrid (levels and dictionary indices)
# ---------------------------------------------------------------------------

def _rle_hybrid(buf: bytes, bit_width: int, count: int) -> list[int]:
    out: list[int] = []
    if bit_width == 0:
        return [0] * count
    reader = _Thrift(buf)
    byte_width = (bit_width + 7) // 8
    mask = (1 << bit_width) - 1
    while len(out) < count:
        if reader.pos >= len(buf):
            raise ParquetUnsupported("RLE stream ended before its values")
        header = reader.varint()
        if header & 1:  # bit-packed groups of 8
            groups = header >> 1
            nbytes = groups * bit_width
            chunk = int.from_bytes(buf[reader.pos:reader.pos + nbytes], "little")
            reader.pos += nbytes
            for i in range(groups * 8):
                out.append((chunk >> (i * bit_width)) & mask)
        else:  # run
            run = header >> 1
            value = int.from_bytes(buf[reader.pos:reader.pos + byte_width], "little")
            reader.pos += byte_width
            out.extend([value] * run)
    return out[:count]


def _bit_width(max_value: int) -> int:
    return max_value.bit_length()


# ---------------------------------------------------------------------------
# Value decoding
# ---------------------------------------------------------------------------

_INT32, _INT64, _INT96, _FLOAT, _DOUBLE, _BYTE_ARRAY = 1, 2, 3, 4, 5, 6
_JULIAN_EPOCH = 2440588  # Julian day number of 1970-01-01
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _plain(buf: bytes, ptype: int, count: int, pos: int = 0) -> tuple[list, int]:
    if ptype == _DOUBLE:
        return list(struct.unpack_from(f"<{count}d", buf, pos)), pos + 8 * count
    if ptype == _FLOAT:
        return list(struct.unpack_from(f"<{count}f", buf, pos)), pos + 4 * count
    if ptype == _INT32:
        return list(struct.unpack_from(f"<{count}i", buf, pos)), pos + 4 * count
    if ptype == _INT64:
        return list(struct.unpack_from(f"<{count}q", buf, pos)), pos + 8 * count
    if ptype == _INT96:
        out = []
        for _ in range(count):
            nanos, jday = struct.unpack_from("<qi", buf, pos)
            pos += 12
            out.append(_EPOCH + timedelta(days=jday - _JULIAN_EPOCH, microseconds=nanos // 1000))
        return out, pos
    if ptype == _BYTE_ARRAY:
        out = []
        for _ in range(count):
            n = struct.unpack_from("<i", buf, pos)[0]
            pos += 4
            out.append(bytes(buf[pos:pos + n]))
            pos += n
        return out, pos
    raise ParquetUnsupported(f"physical type {ptype}")


class _Column:
    def __init__(self, element: dict) -> None:
        self.name = element[4].decode()
        self.ptype = element.get(1)
        self.optional = element.get(3, 0) == 1
        if element.get(3, 0) == 2:
            raise ParquetUnsupported(f"repeated column {self.name}")
        converted = element.get(6)
        logical = element.get(10) or {}
        self.utf8 = converted == 0 or 1 in logical  # UTF8 / StringType
        self.ts_unit = None  # INT64 timestamp unit divisor to microseconds
        if converted == 9:  # TIMESTAMP_MILLIS
            self.ts_unit = ("ms",)
        elif converted == 10:  # TIMESTAMP_MICROS
            self.ts_unit = ("us",)
        elif 8 in logical:  # TimestampType{isAdjustedToUTC, unit}
            unit = logical[8].get(2) or {}
            self.ts_unit = ("ms",) if 1 in unit else ("us",) if 2 in unit else ("ns",)

    def convert(self, value):
        if value is None:
            return None
        if self.ptype == _BYTE_ARRAY and self.utf8:
            return value.decode("utf-8")
        if self.ptype == _INT64 and self.ts_unit:
            unit = self.ts_unit[0]
            micros = value * 1000 if unit == "ms" else value if unit == "us" else value // 1000
            return _EPOCH + timedelta(microseconds=micros)
        return value


def _read_chunk(data: bytes, meta: dict, col: _Column, num_rows: int) -> list:
    codec = meta.get(4, 0)
    if codec not in _CODECS:
        raise ParquetUnsupported(f"{col.name}: compression codec {codec}")
    start = meta.get(11) or meta[9]
    if meta.get(11) and meta[11] > meta[9]:  # a writer bug some tools emit
        start = meta[9]
    end = start + meta[7]
    pos = start
    dictionary: list | None = None
    values: list = []
    max_def = 1 if col.optional else 0
    while pos < end and len(values) < num_rows:
        reader = _Thrift(data, pos)
        header = reader.struct()
        body_start = reader.pos
        compressed = header[3]
        body = data[body_start:body_start + compressed]
        pos = body_start + compressed
        ptype = header[1]
        if ptype == 2:  # DICTIONARY_PAGE
            dph = header[7]
            if dph.get(2, 0) not in (0, 2):  # PLAIN / PLAIN_DICTIONARY
                raise ParquetUnsupported(f"{col.name}: dictionary encoding {dph.get(2)}")
            raw = _decompress(codec, body)
            dictionary, _ = _plain(raw, col.ptype, dph[1])
            continue
        if ptype == 0:  # DATA_PAGE (v1)
            dh = header[5]
            n = dh[1]
            raw = _decompress(codec, body)
            p = 0
            if max_def:
                if dh.get(3, 3) != 3:
                    raise ParquetUnsupported(f"{col.name}: definition level encoding {dh.get(3)}")
                ln = struct.unpack_from("<i", raw, 0)[0]
                defs = _rle_hybrid(raw[4:4 + ln], 1, n)
                p = 4 + ln
            else:
                defs = [0] * n
            encoding = dh[2]
            vals_buf = raw[p:]
        elif ptype == 3:  # DATA_PAGE_V2
            dh = header[8]
            n = dh[1]
            def_len, rep_len = dh[5], dh[6]
            if rep_len:
                raise ParquetUnsupported(f"{col.name}: repetition levels")
            levels = body[:def_len]
            defs = _rle_hybrid(levels, 1, n) if max_def else [0] * n
            rest = body[def_len:]
            vals_buf = _decompress(codec, rest) if dh.get(7, True) else rest
            encoding = dh[4]
        else:
            continue  # INDEX_PAGE carries no values
        present = sum(1 for d in defs if d == max_def)
        if encoding == 0:  # PLAIN
            decoded, _ = _plain(vals_buf, col.ptype, present)
        elif encoding in (2, 8):  # PLAIN_DICTIONARY / RLE_DICTIONARY
            if dictionary is None:
                raise ParquetUnsupported(f"{col.name}: dictionary page missing")
            width = vals_buf[0] if vals_buf else 0
            idx = _rle_hybrid(vals_buf[1:], width, present)
            decoded = [dictionary[i] for i in idx]
        else:
            raise ParquetUnsupported(f"{col.name}: value encoding {encoding}")
        it = iter(decoded)
        values.extend(next(it) if d == max_def else None for d in defs)
    if len(values) != num_rows:
        raise ParquetUnsupported(f"{col.name}: decoded {len(values)} of {num_rows} rows")
    return [col.convert(v) for v in values]


def read_rows(data: bytes, columns: list[str]) -> list[dict]:
    """Every row of a parquet file, as ``{column: value}`` for ``columns``.

    A requested column the file does not carry raises ``KeyError`` naming it,
    so a caller can tell "the export has no such column yet" from "the file
    could not be decoded".
    """
    if data[:4] != b"PAR1" or data[-4:] != b"PAR1":
        raise ParquetUnsupported("not a parquet file (magic bytes)")
    footer_len = struct.unpack_from("<i", data, len(data) - 8)[0]
    footer = _Thrift(data[len(data) - 8 - footer_len:len(data) - 8]).struct()
    schema = footer[2]
    root, leaves = schema[0], schema[1:]
    if root.get(5) != len(leaves) or any(e.get(5) for e in leaves):
        raise ParquetUnsupported("nested schema")
    by_name = {}
    for i, element in enumerate(leaves):
        col = _Column(element)
        by_name[col.name] = (i, col)
    missing = [c for c in columns if c not in by_name]
    if missing:
        raise KeyError(", ".join(missing))
    rows: list[dict] = []
    for rg in footer.get(4) or []:
        n = rg[3]
        chunks = rg[1]
        out_cols = {}
        for name in columns:
            i, col = by_name[name]
            chunk = chunks[i]
            if chunk.get(1):
                raise ParquetUnsupported(f"{name}: column chunk in an external file")
            out_cols[name] = _read_chunk(data, chunk[3], col, n)
        rows.extend({c: out_cols[c][r] for c in columns} for r in range(n))
    return rows


def column_names(data: bytes) -> list[str]:
    """The leaf column names a parquet file carries."""
    footer_len = struct.unpack_from("<i", data, len(data) - 8)[0]
    footer = _Thrift(data[len(data) - 8 - footer_len:len(data) - 8]).struct()
    return [e[4].decode() for e in footer[2][1:]]
