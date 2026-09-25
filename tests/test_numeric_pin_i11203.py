"""D31 computes under a pinned numeric environment (alpha-engine-config-I11203).

Brian's ruling of 2026-09-25: no tolerance may absorb a float difference in the
recompute lineage; the CPU math dispatch is pinned instead. Measured on v1's own
2026-09-23 D31 (see `features/numeric_pin.py`): numpy's AVX-512 ``log`` and its
AVX2-host fallback differ in the last bit, which moved 3,433 `technical` cells by
up to ~6e-13; under the pin one SHA-256 came out on every dispatch setting.

What is asserted here:

A. The pin itself — the variables it sets per numpy version and host, and that
   it refuses to pretend once numpy is already loaded.
B. Every process that computes D31 or its recompute is pinned BEFORE numpy
   loads — executed, not grepped: each entrypoint is run as ``__main__`` in a
   fresh interpreter and the numpy it loaded is inspected.
C. Under the pin, D31's feature code produces the same bytes whatever the
   caller's environment asked for (BLAS threads, BLAS kernel, a narrower SIMD
   mask) — the property the recompute lineage depends on.
D. What is recorded, and the refusal when a run and a recompute disagree.
E. The data-spot box sources the one env file before any workload runs.
"""

from __future__ import annotations

import json
import os
import pathlib
import platform
import subprocess
import sys

import pytest

from features import numeric_pin

REPO = pathlib.Path(__file__).resolve().parents[1]
X86 = platform.machine() in ("x86_64", "AMD64")


def _clean_env(**extra: str) -> dict[str, str]:
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in {numeric_pin.NUMPY_ENV, numeric_pin.BLAS_CORETYPE_ENV, "NPY_DISABLE_CPU_FEATURES", *numeric_pin.THREAD_ENV}
    }
    env.update(extra)
    env["PYTHONPATH"] = str(REPO)
    return env


def _run(code: str, **env: str) -> dict:
    out = subprocess.run(
        [sys.executable, "-c", code], cwd=REPO, env=_clean_env(**env), capture_output=True, text=True, timeout=300
    )
    assert out.returncode == 0, out.stderr[-3000:]
    return json.loads(out.stdout.strip().splitlines()[-1])


# ---------------------------------------------------------------------------
# A. The pin
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "version,names",
    [("1.26.4", "SSE SSE2 SSE3"), ("2.0.2", "SSE SSE2 SSE3"), ("2.3.5", "SSE SSE2 SSE3"), ("2.4.6", "X86_V2"), ("3.0.0", "X86_V2")],
)
def test_numpy_is_limited_to_its_own_baseline_names(version, names):
    env = numeric_pin.pin_environment(version, machine="x86_64", flags=frozenset({"avx2", "fma"}))
    assert env[numeric_pin.NUMPY_ENV] == names
    assert env[numeric_pin.BLAS_CORETYPE_ENV] == "Haswell"
    assert {k: env[k] for k in numeric_pin.THREAD_ENV} == {k: "1" for k in numeric_pin.THREAD_ENV}


def test_a_host_below_the_floor_is_not_given_kernels_it_cannot_run():
    env = numeric_pin.pin_environment("1.26.4", machine="x86_64", flags=frozenset({"sse4_2", "avx"}))
    assert numeric_pin.BLAS_CORETYPE_ENV not in env  # Haswell kernels would fault there
    assert env[numeric_pin.NUMPY_ENV] == "SSE SSE2 SSE3"


def test_apply_refuses_once_numpy_is_loaded_under_another_environment(monkeypatch):
    import numpy  # noqa: F401 - loaded, as in any test process

    for key in numeric_pin.pin_environment():
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(numeric_pin.NumericPinError, match="imported before the numeric pin"):
        numeric_pin.apply()
    for key, value in numeric_pin.pin_environment().items():
        monkeypatch.setenv(key, value)
    assert numeric_pin.apply() == numeric_pin.pin_environment()  # already in effect: a no-op


def test_the_shell_exports_are_derived_from_the_one_definition():
    lines = numeric_pin.shell_exports().splitlines()
    # numpy will not import with both NPY_ENABLE_ and NPY_DISABLE_CPU_FEATURES set.
    assert lines[0] == "unset NPY_DISABLE_CPU_FEATURES"
    assert lines[1:] == [f"export {k}={v}" if " " not in v else f"export {k}='{v}'"
                         for k, v in sorted(numeric_pin.pin_environment().items())]


# ---------------------------------------------------------------------------
# B. Every entrypoint pins before numpy loads
# ---------------------------------------------------------------------------

_INSPECT = """
import json, runpy, sys
sys.argv = {argv!r}
try:
    {runner}
except SystemExit:
    pass
# Whatever the entrypoint went on to load, numpy loads under what it left in place.
import numpy  # noqa: F401
from features import numeric_pin
e = numeric_pin.effective()
print(json.dumps({{"dispatch": e["numpy_dispatch"], "env": e["env"], "pinned": e["pinned"], "problems": e["problems"]}}))
"""

_ENTRYPOINTS = {
    # v1's D31 (`post-market-data`) and every other data-spot workload.
    "weekly_collector.py": ("runpy.run_path('weekly_collector.py', run_name='__main__')", ["weekly_collector.py", "--help"]),
    # The shadow's D31 (`shadow run --module weekly_collector`) and `shadow recompute-lineage`.
    "python -m shadow": ("runpy.run_module('shadow', run_name='__main__', alter_sys=True)", ["shadow", "--help"]),
    # D31 by hand.
    "python -m features.compute": (
        "runpy.run_module('features.compute', run_name='__main__', alter_sys=True)",
        ["compute", "--help"],
    ),
}


@pytest.mark.skipif(not X86, reason="the pin names x86-64 numpy features")
@pytest.mark.parametrize("entrypoint", sorted(_ENTRYPOINTS))
def test_each_d31_entrypoint_runs_pinned(entrypoint):
    runner, argv = _ENTRYPOINTS[entrypoint]
    seen = _run(_INSPECT.format(argv=argv, runner=runner))
    assert seen["dispatch"] == [], f"{entrypoint}: numpy loaded with {seen['dispatch']} dispatched"
    pinned = numeric_pin.pin_environment()
    assert {k: seen["env"].get(k) for k in pinned} == pinned
    if all(f in numeric_pin.cpu_flags() for f in numeric_pin.HOST_FLOOR):
        assert seen["pinned"], seen["problems"]


def test_an_importer_is_not_pinned_behind_its_back():
    """The pin is an entrypoint's act: importing the module (a test, a notebook)
    leaves that process's environment alone."""
    seen = _run(
        "import json, os, weekly_collector; print(json.dumps({k: os.environ.get(k) for k in "
        f"{sorted(numeric_pin.pin_environment())!r}}}))"
    )
    assert set(seen.values()) == {None}


# ---------------------------------------------------------------------------
# C. Under the pin, the bytes do not depend on what the caller asked for
# ---------------------------------------------------------------------------

_COMPUTE = """
from features import numeric_pin
numeric_pin.apply()
import hashlib, json
import numpy as np, pandas as pd
from features.feature_engineer import compute_features
rng = np.random.default_rng(20260923)
idx = pd.bdate_range("2025-01-02", periods=420)
spy = pd.Series(500 * np.exp(np.cumsum(rng.normal(3e-4, 0.011, len(idx)))), index=idx)
h = hashlib.sha256()
cols = ["beta_60d", "idio_vol_60d", "vol_ratio_10_60", "residual_momentum_ratio"]
for t in range(12):
    close = 50 * np.exp(np.cumsum(rng.normal(2e-4, 0.02, len(idx))))
    df = pd.DataFrame({"Open": close, "High": close * 1.01, "Low": close * 0.99, "Close": close,
                       "Volume": rng.integers(1e5, 1e7, len(idx)).astype(float)}, index=idx)
    out = compute_features(df, spy_series=spy)
    h.update(out[cols].to_numpy(dtype="float64").tobytes())
e = numeric_pin.effective()
print(json.dumps({"sha": h.hexdigest(), "pinned": e["pinned"], "dispatch": e["numpy_dispatch"], "blas": e["blas"]}))
"""


@pytest.mark.skipif(not X86, reason="the pin names x86-64 numpy features")
def test_d31s_log_return_columns_are_identical_whatever_the_caller_asked_for():
    asked = [
        {},
        {"OPENBLAS_NUM_THREADS": "4", "OMP_NUM_THREADS": "4", "OPENBLAS_CORETYPE": "SkylakeX"},
        # A narrower CPU's SIMD mask (numpy 2.4 and <2.4 spellings; the unknown half is ignored).
        {"NPY_DISABLE_CPU_FEATURES": "X86_V4 AVX512F AVX512_SKX"},
    ]
    runs = [_run(_COMPUTE, **env) for env in asked]
    assert all(r["dispatch"] == [] for r in runs)
    assert all(r["blas"] is None or (r["blas"]["core"], r["blas"]["threads"]) == ("Haswell", 1) for r in runs)
    assert len({r["sha"] for r in runs}) == 1, runs


# ---------------------------------------------------------------------------
# D. The record and the refusal
# ---------------------------------------------------------------------------


def _environment(**overrides):
    base = {
        "policy": numeric_pin.POLICY,
        "pinned": True,
        "problems": [],
        "env": {numeric_pin.NUMPY_ENV: "SSE SSE2 SSE3"},
        "numpy": "1.26.4",
        "numpy_dispatch": [],
        "blas": {"core": "Haswell", "threads": 1, "config": "OpenBLAS 0.3.23.dev"},
        "libc": "glibc 2.34",
        "libm_path": "fma",
        "cpu_model": "Intel(R) Xeon(R) Platinum 8124M CPU @ 3.00GHz",
        "cpu_vendor": "GenuineIntel",
        "machine": "x86_64",
        "python": "3.12.11",
        "instance_type": "c5.large",
    }
    return {**base, **overrides}


def test_the_record_is_a_closed_shape_input_ref_that_reads_back_field_for_field():
    ref = numeric_pin.as_input_ref(_environment())
    assert set(ref) == {"key", "etag", "version", "schema_version"}
    assert ref["version"].startswith(f"numeric-pin:{numeric_pin.POLICY}:pinned:")
    fields = numeric_pin.from_input_ref(ref["key"])
    assert fields == numeric_pin.flat(_environment())
    assert fields["cpu_model"].startswith("Intel(R) Xeon(R) Platinum 8124M")
    assert fields["instance_type"] == "c5.large"
    assert numeric_pin.from_input_ref("s3://b/k") is None


def test_the_signature_ignores_the_cpu_but_not_the_stack():
    intel = numeric_pin.as_input_ref(_environment())["version"]
    amd = numeric_pin.as_input_ref(
        _environment(cpu_model="AMD EPYC 7R32", cpu_vendor="AuthenticAMD", instance_type="c5a.large")
    )["version"]
    assert intel == amd
    assert numeric_pin.as_input_ref(_environment(numpy="2.4.6"))["version"] != intel


def test_mismatch_names_every_reason():
    own = numeric_pin.flat(_environment())
    assert numeric_pin.mismatch(own, own) is None
    assert "recorded no numeric environment" in numeric_pin.mismatch(None, own)
    unpinned = numeric_pin.flat(_environment(pinned=False, problems=["numpy dispatches ['AVX512F'] above its baseline"]))
    assert "UNPINNED on c5.large" in numeric_pin.mismatch(unpinned, own)
    assert "this recompute is not pinned" in numeric_pin.mismatch(own, unpinned)
    why = numeric_pin.mismatch(own, numeric_pin.flat(_environment(libm_path="no-fma", numpy="2.4.6")))
    assert "libm_path 'fma' vs 'no-fma'" in why and "numpy '1.26.4' vs '2.4.6'" in why


def test_effective_measures_this_process_rather_than_trusting_the_request():
    """In a test process numpy loaded without the pin: `effective` must say so."""
    import numpy  # noqa: F401

    e = numeric_pin.effective()
    if os.environ.get(numeric_pin.NUMPY_ENV) != numeric_pin.pin_environment().get(numeric_pin.NUMPY_ENV):
        assert not e["pinned"]
        assert any(numeric_pin.NUMPY_ENV in p for p in e["problems"])


# ---------------------------------------------------------------------------
# E. The data-spot box
# ---------------------------------------------------------------------------


def _dispatcher():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_data_spot_index_numeric_pin", REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("workload", ["post-market-data", "shadow-sameday"])
def test_the_box_sources_the_env_file_before_the_workload(workload):
    module = _dispatcher()
    rendered = module._bootstrap_command(workload, module._WORKLOADS[workload], "tok")
    source = "source infrastructure/data_box_env.sh"
    assert source in rendered
    assert rendered.index("source .venv/bin/activate") < rendered.index(source)
    assert rendered.index(source) < rendered.index(module._WORKLOADS[workload])


def _source_box_env(tmp_path, imds_endpoint):
    """Source the env file under the box's shell options, with `python` on PATH as the venv gives it."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    (bin_dir / "python").symlink_to(sys.executable)
    script = (
        "set -uo pipefail; source infrastructure/data_box_env.sh || exit 9; "
        "env | grep -E '^(NPY_|OPENBLAS_|OMP_|MKL_|NE_DATA_)' | sort"
    )
    env = {
        **_clean_env(NPY_DISABLE_CPU_FEATURES="X86_V4"),  # a caller's mask the pin must clear
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
        "NE_DATA_IMDS_ENDPOINT": imds_endpoint,
    }
    for key in ("NE_DATA_INSTANCE_TYPE", "NE_DATA_LIFECYCLE"):
        env.pop(key, None)
    out = subprocess.run(["bash", "-c", script], cwd=REPO, capture_output=True, text=True, timeout=60, env=env)
    assert out.returncode == 0, out.stderr + out.stdout
    exported = dict(line.split("=", 1) for line in out.stdout.splitlines() if "=" in line and not line.startswith("["))
    for key, value in numeric_pin.pin_environment().items():
        assert exported.get(key) == value, key
    assert "NPY_DISABLE_CPU_FEATURES" not in exported
    return out.stdout, exported


def test_the_env_file_derives_the_pin_and_says_so_when_metadata_is_unreachable(tmp_path):
    """No metadata: the compute row is left to the manifest's own `local`, loudly — never guessed."""
    stdout, exported = _source_box_env(tmp_path, "http://127.0.0.1:9")
    assert "NE_DATA_INSTANCE_TYPE" not in exported
    assert "compute.instance_type=local" in stdout


def test_the_env_file_declares_the_compute_row_from_instance_metadata(tmp_path):
    """IMDSv2, as on the box: a token PUT, then the two reads with the token."""
    import http.server
    import threading

    class _Imds(http.server.BaseHTTPRequestHandler):
        def do_PUT(self):  # noqa: N802 - http.server spelling
            ok = self.path == "/latest/api/token" and self.headers.get("X-aws-ec2-metadata-token-ttl-seconds")
            self._reply(200 if ok else 400, "tok-123")

        def do_GET(self):  # noqa: N802
            if self.headers.get("X-aws-ec2-metadata-token") != "tok-123":
                return self._reply(401, "")
            answers = {"/latest/meta-data/instance-type": "c6a.large", "/latest/meta-data/instance-life-cycle": "spot"}
            self._reply(200 if self.path in answers else 404, answers.get(self.path, ""))

        def _reply(self, code, body):
            self.send_response(code)
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Imds)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        stdout, exported = _source_box_env(tmp_path, f"http://127.0.0.1:{server.server_address[1]}")
    finally:
        server.shutdown()
    assert exported["NE_DATA_INSTANCE_TYPE"] == "c6a.large"
    assert exported["NE_DATA_LIFECYCLE"] == "spot"
    assert "instance_type=c6a.large lifecycle=spot" in stdout
