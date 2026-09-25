"""The numeric environment D31 computes under: pinned, so its bytes do not depend on the CPU.

`alpha-engine-config-I11203` (recompute lineage), Brian's ruling of 2026-09-25:
no tolerance and no widened band may absorb a float difference, and the CPU
math dispatch is pinned instead, accepting a one-time shift of ~1e-13 in the
affected `technical` values.

WHAT MOVES THE BYTES, measured 2026-09-25 by re-running v1's own 2026-09-23 D31
over its recorded inputs (4,920 cells of the four log-return columns
`beta_60d`, `idio_vol_60d`, `vol_ratio_10_60`, `residual_momentum_ratio`):

* **numpy's runtime SIMD dispatch.** ``np.log`` on float64 runs an AVX-512F
  kernel on a host that has AVX-512 (c5/c6i/m5/m6i/r5/r6i, Intel; c7a, AMD) and
  glibc's scalar ``log`` on a host that does not (c5a/m5a/r5a/c6a, AMD). The
  two differ in the last bit for some inputs, and a 60-row rolling covariance
  carries that to ~1e-13. Same inputs, same code, numpy 1.26.4: AVX-512 on
  reproduced v1's published `technical.parquet` byte for byte (v1 ran on a
  c5.large); AVX-512 masked (an AVX2-only host) differed in 3,433 cells.
  Nothing else in D31 is dispatch-sensitive: the rolling statistics are pandas'
  own Cython loops and ``+ - * /`` are exact IEEE operations.
* **glibc's libm variant.** With numpy's kernel out of the way, ``log`` is
  glibc's, and glibc picks an FMA or a non-FMA build of it by CPU (``ifunc``).
  Masking FMA/AVX2 from glibc (``GLIBC_TUNABLES``) moved 24 cells. Every
  instance type the data-spot box may launch as has FMA and AVX2 (x86-64-v3),
  so that choice is the same everywhere; it is recorded and checked
  (:data:`HOST_FLOOR`), not assumed.
* **BLAS kernel and thread count.** D31 makes no BLAS call today (no ``dot``,
  ``lstsq`` or ``polyfit`` reaches a feature group). OpenBLAS picks a kernel per
  CPU (SkylakeX vs Haswell) and splits work across threads, and both change a
  float64 ``dot``/``gemm`` result (measured on this host), so the pin fixes
  them too rather than leaving the next BLAS call to discover it.

THE PIN: numpy dispatches nothing above its build's baseline
(``NPY_ENABLE_CPU_FEATURES`` naming only baseline features), OpenBLAS runs its
``Haswell`` kernels on one thread, OpenMP/MKL on one thread. Under it the
2026-09-23 `technical` group came out identical — one SHA-256 — with AVX-512
on, AVX-512 masked, BLAS at 1 or 4 threads and SkylakeX or Haswell kernels, on
numpy 1.26.4 and on numpy 2.4.6.

WHY PYTHON, NOT THE SHELL: numpy and OpenBLAS read these variables once, when
numpy is first imported. Every process that runs D31 or its recompute calls
:func:`apply` as its first statement (``weekly_collector.py``,
``python -m shadow``, ``python -m features.compute``), so the pin holds however
the process was launched — the data-spot dispatcher, ``spot_data_weekly.sh``,
or by hand — and this module is its only definition. The box's shell
environment is derived from it (``python -m features.numeric_pin
--shell-exports``), never restated.

WHAT IS RECORDED: :func:`effective` measures what is actually in effect after
numpy loaded — never what was asked for — and D31 writes it into its run
manifest's ``inputs`` as one ``numeric-env://`` entry (:func:`as_input_ref`).
The recompute refuses, by name, a run whose recorded environment is unpinned
or differs from its own (:func:`mismatch`).

Stdlib only at import: this module must be importable before numpy is.
"""

from __future__ import annotations

import hashlib
import os
import platform
import shlex
import sys
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit

#: Bump when what the pin sets changes; a run and a recompute under different
#: policies are not comparable.
POLICY = "d31-numeric-pin.v1"

#: numpy: enable only these runtime-dispatch features, i.e. nothing above the
#: build's baseline. The names are the build's own baseline names, which numpy
#: renamed in 2.4 (x86-64 wheels: ``SSE SSE2 SSE3`` before, ``X86_V2`` from).
NUMPY_ENV = "NPY_ENABLE_CPU_FEATURES"
#: numpy refuses to import with both set, and the pin owns the choice: removed.
NUMPY_DISABLE_ENV = "NPY_DISABLE_CPU_FEATURES"
_NUMPY_BASELINE_BEFORE_2_4 = "SSE SSE2 SSE3"
_NUMPY_BASELINE_FROM_2_4 = "X86_V2"

#: One thread everywhere a BLAS/OpenMP runtime could split a reduction.
THREAD_ENV = {"OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1"}

#: OpenBLAS kernel family. Haswell (AVX2+FMA) runs on every host above the
#: floor; it is also what OpenBLAS picks for AMD Zen on its own.
BLAS_CORETYPE_ENV = "OPENBLAS_CORETYPE"
BLAS_CORETYPE = "Haswell"

#: `/proc/cpuinfo` flags a host must have for the pin to mean the same thing
#: on it: glibc selects its FMA ``log`` from them, and the Haswell BLAS
#: kernels need them.
HOST_FLOOR = ("avx2", "fma")

#: The recorded fields a run and its recompute must agree on. The CPU model
#: and instance type are recorded but deliberately NOT here: making them
#: irrelevant is what the pin is for.
SIGNATURE_FIELDS = (
    "policy",
    "pinned",
    "numpy",
    "numpy_dispatch",
    "blas_core",
    "blas_threads",
    "libc",
    "libm_path",
)

INPUT_SCHEME = "numeric-env://"
INPUT_VERSION_PREFIX = "numeric-pin:"


class NumericPinError(RuntimeError):
    """numpy was imported before the pin could be applied, under a different environment."""


# ---------------------------------------------------------------------------
# What the pin sets
# ---------------------------------------------------------------------------


def _numpy_version() -> str | None:
    try:
        from importlib.metadata import version

        return version("numpy")
    except Exception:  # noqa: BLE001 - no numpy installed: nothing to pin
        return None


def numpy_baseline_names(numpy_version: str) -> str:
    """The baseline feature names an x86-64 numpy wheel of ``numpy_version`` uses."""
    parts = []
    for piece in numpy_version.split(".")[:2]:
        digits = "".join(ch for ch in piece if ch.isdigit())
        parts.append(int(digits or 0))
    major, minor = (parts + [0, 0])[:2]
    return _NUMPY_BASELINE_FROM_2_4 if (major, minor) >= (2, 4) else _NUMPY_BASELINE_BEFORE_2_4


def cpu_flags() -> frozenset[str]:
    """The host's `/proc/cpuinfo` flags (empty where there is none)."""
    try:
        with open("/proc/cpuinfo", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if line.startswith("flags"):
                    return frozenset(line.split(":", 1)[1].split())
    except OSError:
        pass
    return frozenset()


def _cpu_model() -> tuple[str, str]:
    model = vendor = ""
    try:
        with open("/proc/cpuinfo", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if not model and line.startswith("model name"):
                    model = line.split(":", 1)[1].strip()
                elif not vendor and line.startswith("vendor_id"):
                    vendor = line.split(":", 1)[1].strip()
                if model and vendor:
                    break
    except OSError:
        pass
    return model or platform.processor() or "unknown", vendor or "unknown"


def pin_environment(
    numpy_version: str | None = None,
    *,
    machine: str | None = None,
    flags: frozenset[str] | None = None,
) -> dict[str, str]:
    """The exact variables the pin sets on this host. Pure given its arguments.

    A host without the :data:`HOST_FLOOR` gets no ``OPENBLAS_CORETYPE``: forcing
    Haswell kernels onto it would fault. :func:`effective` reports such a host
    as not pinned rather than pretending.
    """
    machine = machine or platform.machine()
    flags = cpu_flags() if flags is None else flags
    numpy_version = numpy_version or _numpy_version()
    env = dict(THREAD_ENV)
    if machine in ("x86_64", "AMD64") and numpy_version:
        env[NUMPY_ENV] = numpy_baseline_names(numpy_version)
        if all(f in flags for f in HOST_FLOOR):
            env[BLAS_CORETYPE_ENV] = BLAS_CORETYPE
    return env


def apply() -> dict[str, str]:
    """Set the pin in this process's environment. Must run before numpy is imported.

    Idempotent: once numpy is loaded it only checks that the environment it
    loaded under is the pin's (the ``python -m shadow run --module
    weekly_collector`` path applies it twice). Raises :class:`NumericPinError`
    when numpy was loaded under anything else — the pin can no longer take
    effect, and a run that silently computed unpinned is what this exists to
    prevent.
    """
    env = pin_environment()
    if "numpy" in sys.modules:
        stale = {k: os.environ.get(k) for k, v in env.items() if os.environ.get(k) != v}
        if os.environ.get(NUMPY_DISABLE_ENV):
            stale[NUMPY_DISABLE_ENV] = os.environ[NUMPY_DISABLE_ENV]
        if stale:
            raise NumericPinError(
                f"numpy was imported before the numeric pin was applied, under {stale}; the pin "
                f"({POLICY}) sets {env}. Call features.numeric_pin.apply() before anything imports numpy."
            )
        return env
    os.environ.pop(NUMPY_DISABLE_ENV, None)
    os.environ.update(env)
    return env


def shell_exports() -> str:
    """``export K=V`` lines for a shell, derived from :func:`pin_environment`."""
    lines = [f"unset {NUMPY_DISABLE_ENV}"]
    lines += [f"export {k}={shlex.quote(v)}" for k, v in sorted(pin_environment().items())]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# What is actually in effect
# ---------------------------------------------------------------------------


def _openblas() -> dict[str, Any] | None:
    """The loaded OpenBLAS's kernel family, thread count and build config, asked of the library itself."""
    import ctypes

    try:
        with open("/proc/self/maps", encoding="utf-8", errors="replace") as fh:
            paths = sorted({line.split()[-1] for line in fh if "openblas" in line.lower() and ".so" in line})
    except OSError:
        return None
    for path in paths:
        try:
            lib = ctypes.CDLL(path)
        except OSError:
            continue
        for prefix in ("scipy_openblas_", "openblas_"):
            for suffix in ("64_", ""):
                try:
                    core = getattr(lib, f"{prefix}get_corename{suffix}")
                    threads = getattr(lib, f"{prefix}get_num_threads{suffix}")
                    config = getattr(lib, f"{prefix}get_config{suffix}")
                except AttributeError:
                    continue
                core.restype = ctypes.c_char_p
                config.restype = ctypes.c_char_p
                return {
                    "core": (core() or b"").decode(errors="replace"),
                    "threads": int(threads()),
                    "config": (config() or b"").decode(errors="replace").strip(),
                }
    return None


def effective() -> dict[str, Any]:
    """What this process is actually computing under (imports numpy).

    ``pinned`` is True only when every variable the pin sets is set to the
    pin's value, numpy dispatches no feature above its baseline, the loaded
    OpenBLAS (when one is found) runs the pinned kernel on one thread, and the
    host meets :data:`HOST_FLOOR`. ``problems`` names each shortfall.
    """
    import numpy

    try:
        from numpy._core._multiarray_umath import __cpu_baseline__, __cpu_dispatch__, __cpu_features__
    except ImportError:  # numpy < 2
        from numpy.core._multiarray_umath import __cpu_baseline__, __cpu_dispatch__, __cpu_features__

    flags = cpu_flags()
    model, vendor = _cpu_model()
    expected = pin_environment(numpy.__version__, flags=flags)
    problems: list[str] = []
    for key, value in sorted(expected.items()):
        if os.environ.get(key) != value:
            problems.append(f"{key} is {os.environ.get(key)!r}, the pin sets {value!r}")
    if os.environ.get(NUMPY_DISABLE_ENV):
        problems.append(f"{NUMPY_DISABLE_ENV} is set ({os.environ[NUMPY_DISABLE_ENV]!r}); the pin owns numpy's dispatch")
    dispatch = [f for f in __cpu_dispatch__ if __cpu_features__.get(f)]
    if dispatch:
        problems.append(f"numpy dispatches {dispatch} above its baseline")
    below = [f for f in HOST_FLOOR if f not in flags]
    if below:
        problems.append(f"the host lacks {below}, below the pin's floor {list(HOST_FLOOR)}")
    blas = _openblas()
    if blas is not None:
        if blas["core"].lower() != BLAS_CORETYPE.lower():
            problems.append(f"OpenBLAS runs its {blas['core']} kernels, the pin sets {BLAS_CORETYPE}")
        if blas["threads"] != 1:
            problems.append(f"OpenBLAS runs {blas['threads']} threads, the pin sets 1")
    return {
        "policy": POLICY,
        "pinned": not problems,
        "problems": problems,
        "env": {
            k: os.environ.get(k) for k in sorted({*expected, NUMPY_ENV, NUMPY_DISABLE_ENV, BLAS_CORETYPE_ENV, *THREAD_ENV})
        },
        "numpy": numpy.__version__,
        "numpy_baseline": list(__cpu_baseline__),
        "numpy_dispatch": dispatch,
        "blas": blas,
        "libc": " ".join(p for p in platform.libc_ver() if p) or "unknown",
        # glibc's `log` ifunc: the FMA build on every host with FMA+AVX2.
        "libm_path": "fma" if not below else "no-fma",
        "cpu_model": model,
        "cpu_vendor": vendor,
        "machine": platform.machine(),
        "python": platform.python_version(),
        "instance_type": os.environ.get("NE_DATA_INSTANCE_TYPE") or "local",
    }


def flat(eff: Mapping[str, Any]) -> dict[str, str]:
    """The recorded form: every field a string, the same on write and on read-back."""
    blas = eff.get("blas") or {}
    env = eff.get("env") or {}
    return {
        "policy": str(eff.get("policy") or ""),
        "pinned": "1" if eff.get("pinned") else "0",
        "problems": "; ".join(eff.get("problems") or [])[:600],
        "npy_enable": str(env.get(NUMPY_ENV) or ""),
        "numpy": str(eff.get("numpy") or ""),
        "numpy_dispatch": " ".join(eff.get("numpy_dispatch") or []),
        "blas_core": str(blas.get("core") or "none"),
        "blas_threads": str(blas.get("threads") if blas else "none"),
        "blas_config": str(blas.get("config") or "")[:200],
        "libc": str(eff.get("libc") or ""),
        "libm_path": str(eff.get("libm_path") or ""),
        "cpu_model": str(eff.get("cpu_model") or ""),
        "cpu_vendor": str(eff.get("cpu_vendor") or ""),
        "machine": str(eff.get("machine") or ""),
        "python": str(eff.get("python") or ""),
        "instance_type": str(eff.get("instance_type") or ""),
    }


def as_input_ref(eff: Mapping[str, Any]) -> dict[str, Any]:
    """The run manifest ``InputRef`` (closed shape: key, etag, version, schema_version)."""
    fields = flat(eff)
    signature = hashlib.sha256(
        "\n".join(f"{k}={fields[k]}" for k in SIGNATURE_FIELDS).encode()
    ).hexdigest()[:16]
    state = "pinned" if fields["pinned"] == "1" else "unpinned"
    return {
        "key": f"{INPUT_SCHEME}process?{urlencode(fields)}",
        "etag": None,
        "version": f"{INPUT_VERSION_PREFIX}{fields['policy']}:{state}:{signature}",
        "schema_version": None,
    }


def from_input_ref(key: str) -> dict[str, str] | None:
    """Read a :func:`as_input_ref` key back to its fields; None when it is not one."""
    if not key.startswith(INPUT_SCHEME):
        return None
    return dict(parse_qsl(urlsplit(key).query, keep_blank_values=True))


def mismatch(recorded: Mapping[str, str] | None, own: Mapping[str, str]) -> str | None:
    """Why a run computed under ``recorded`` cannot be reproduced under ``own``; None if it can."""
    if recorded is None:
        return (
            "the run recorded no numeric environment: it computed before the numeric pin "
            f"({POLICY}), so its float bytes depend on the CPU it ran on"
        )
    if recorded.get("pinned") != "1":
        return (
            f"the run computed UNPINNED on {recorded.get('instance_type') or '?'} "
            f"({recorded.get('cpu_model') or '?'}): {recorded.get('problems') or 'no reason recorded'}"
        )
    if own.get("pinned") != "1":
        return f"this recompute is not pinned: {own.get('problems') or 'no reason given'}"
    diffs = [f"{k} {recorded.get(k)!r} vs {own.get(k)!r}" for k in SIGNATURE_FIELDS if recorded.get(k) != own.get(k)]
    if diffs:
        return "the run's numeric environment differs from the recompute's (run vs recompute): " + "; ".join(diffs)
    return None


def main(argv: list[str] | None = None) -> int:
    import argparse
    import json

    parser = argparse.ArgumentParser(prog="python -m features.numeric_pin", description=__doc__.split("\n")[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--shell-exports", action="store_true", help="print `export` lines for the pin")
    group.add_argument("--effective", action="store_true", help="apply the pin, import numpy, print what is in effect")
    args = parser.parse_args(argv)
    if args.shell_exports:
        print(shell_exports())
        return 0
    apply()
    eff = effective()
    print(json.dumps(eff, indent=1, sort_keys=True))
    return 0 if eff["pinned"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
