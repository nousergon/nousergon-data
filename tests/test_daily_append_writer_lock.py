"""Pin the universe_writer_lock wire-up in builders/daily_append.py.

Origin: ROADMAP L286b — closes the producer-side half of the same
single-writer-per-resource invariant the L274 SF MutualExclusionGuard
(DynamoDB-side) covers at the SF entry point. Without this lock, an
operator-launched manual ``python -m builders.daily_append`` run would
race the SF-driven path at ArcticDB exactly like the 2026-05-26
dup-EB-target incident (321 unique-symbol
``E_NON_INCREASING_INDEX_VERSION`` races, 35.6% error rate, missed
trading day).

The lock is **default-OFF** via
``ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED`` for safe rollout — these
tests pin both polarities:

* env unset (default) → lock NOT acquired; ``_daily_append_impl``
  called directly (existing behavior, byte-identical to v0.37 lib pin)
* env truthy → lock IS acquired; impl called inside the ``with`` block
* ``dry_run=True`` always bypasses the lock regardless of env

Composes with the alpha-engine-lib v0.38.0 ``locks`` module's own unit
tests (acquire / release / TTL recovery / contention). This file pins
the integration shape: the wire-up calls the lib API correctly.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from builders import daily_append as daily_append_module


@pytest.fixture
def stub_impl():
    """Replace ``_daily_append_impl`` with a MagicMock so the wrapper's
    routing is observable without exercising the 800-line body."""
    with patch.object(
        daily_append_module, "_daily_append_impl", return_value={"ok": True}
    ) as stub:
        yield stub


@pytest.fixture
def clear_lock_env(monkeypatch):
    """Ensure the env var is unset at the start of each test."""
    monkeypatch.delenv(
        "ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED", raising=False
    )


# ── Default-OFF semantics ────────────────────────────────────────────────


class TestDefaultOffRollout:
    def test_env_unset_does_not_acquire_lock(self, stub_impl, clear_lock_env):
        """No env var → existing behavior. Impl is called WITHOUT going
        through ``universe_writer_lock``; no boto3 S3 client is built
        for the lock."""
        with patch(
            "nousergon_lib.locks.universe_writer_lock"
        ) as mock_lock:
            result = daily_append_module.daily_append(date_str="2026-05-27")
        assert result == {"ok": True}
        mock_lock.assert_not_called()
        stub_impl.assert_called_once()

    @pytest.mark.parametrize(
        "falsy_value", ["", "false", "False", "0", "no", "off", "disabled"]
    )
    def test_env_falsy_value_does_not_acquire_lock(
        self, stub_impl, monkeypatch, falsy_value
    ):
        monkeypatch.setenv(
            "ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED", falsy_value
        )
        with patch(
            "nousergon_lib.locks.universe_writer_lock"
        ) as mock_lock:
            daily_append_module.daily_append(date_str="2026-05-27")
        mock_lock.assert_not_called()


# ── Lock acquisition when enabled ────────────────────────────────────────


class TestLockAcquisitionWhenEnabled:
    @pytest.mark.parametrize("truthy_value", ["1", "true", "True", "yes", "YES"])
    def test_env_truthy_acquires_lock_with_writer_id(
        self, stub_impl, monkeypatch, truthy_value
    ):
        """Env truthy → wire-up calls
        ``universe_writer_lock(writer_id=...)`` exactly once. The writer_id
        format is pinned so operators inspecting the live lock body
        recognize the holder shape."""
        monkeypatch.setenv(
            "ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED", truthy_value
        )
        monkeypatch.setenv("USER", "testop")
        with patch(
            "nousergon_lib.locks.universe_writer_lock"
        ) as mock_lock:
            mock_lock.return_value.__enter__ = MagicMock(
                return_value=MagicMock()
            )
            mock_lock.return_value.__exit__ = MagicMock(return_value=False)
            daily_append_module.daily_append(date_str="2026-05-27")
        mock_lock.assert_called_once()
        kwargs = mock_lock.call_args.kwargs
        assert "writer_id" in kwargs
        assert kwargs["writer_id"].startswith("daily_append-testop-pid")

    def test_impl_called_inside_lock_context(
        self, stub_impl, monkeypatch
    ):
        """The lock's ``__enter__`` must precede ``_daily_append_impl``'s
        call; ``__exit__`` must follow. Pins the with-block ordering so
        a future refactor that moves the impl call OUTSIDE the with
        block fails loud."""
        monkeypatch.setenv(
            "ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED", "true"
        )
        calls: list[str] = []
        with patch(
            "nousergon_lib.locks.universe_writer_lock"
        ) as mock_lock:
            mock_lock.return_value.__enter__ = MagicMock(
                side_effect=lambda: calls.append("enter") or MagicMock()
            )
            mock_lock.return_value.__exit__ = MagicMock(
                side_effect=lambda *a: calls.append("exit") or False
            )
            stub_impl.side_effect = lambda **k: (
                calls.append("impl") or {"ok": True}
            )
            daily_append_module.daily_append(date_str="2026-05-27")
        assert calls == ["enter", "impl", "exit"]


# ── dry_run bypass ───────────────────────────────────────────────────────


class TestDryRunBypassesLock:
    def test_dry_run_skips_lock_even_when_env_enabled(
        self, stub_impl, monkeypatch
    ):
        """dry_run paths are read-only and must NEVER take the lock —
        otherwise operator inspection during a live Saturday SF would
        block on the running writer's lease."""
        monkeypatch.setenv(
            "ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED", "true"
        )
        with patch(
            "nousergon_lib.locks.universe_writer_lock"
        ) as mock_lock:
            daily_append_module.daily_append(
                date_str="2026-05-27", dry_run=True
            )
        mock_lock.assert_not_called()


# ── Arg forwarding ───────────────────────────────────────────────────────


class TestArgForwarding:
    def test_all_kwargs_passed_through_lock_path(
        self, stub_impl, monkeypatch
    ):
        """Every public parameter MUST forward unchanged across the
        wrapper. A regression that drops a kwarg (or swaps positional
        order) silently changes behavior on the LOCK path only."""
        monkeypatch.setenv(
            "ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED", "true"
        )
        with patch(
            "nousergon_lib.locks.universe_writer_lock"
        ) as mock_lock:
            mock_lock.return_value.__enter__ = MagicMock(
                return_value=MagicMock()
            )
            mock_lock.return_value.__exit__ = MagicMock(return_value=False)
            daily_append_module.daily_append(
                date_str="2026-05-27",
                bucket="custom-bucket",
                dry_run=False,
                skip_if_exists=True,
                expected_tickers=["AAPL", "MSFT"],
            )
        stub_impl.assert_called_once_with(
            date_str="2026-05-27",
            bucket="custom-bucket",
            dry_run=False,
            skip_if_exists=True,
            expected_tickers=["AAPL", "MSFT"],
        )

    def test_all_kwargs_passed_through_no_lock_path(
        self, stub_impl, clear_lock_env
    ):
        """Same coverage for the default-OFF code path — drop-a-kwarg
        regressions must surface regardless of env-var state."""
        daily_append_module.daily_append(
            date_str="2026-05-27",
            bucket="custom-bucket",
            dry_run=False,
            skip_if_exists=True,
            expected_tickers=["AAPL", "MSFT"],
        )
        stub_impl.assert_called_once_with(
            date_str="2026-05-27",
            bucket="custom-bucket",
            dry_run=False,
            skip_if_exists=True,
            expected_tickers=["AAPL", "MSFT"],
        )


# ── Lock-held propagation ────────────────────────────────────────────────


class TestLockHeldPropagates:
    def test_lock_held_error_propagates_to_caller(
        self, stub_impl, monkeypatch
    ):
        """If the lib's lock acquisition raises
        ``LockHeldByAnotherWriterError``, that exception MUST propagate
        unchanged — per ``~/Development/CLAUDE.md`` no-silent-fails
        rule. The wrapper does not catch / convert the exception."""
        from nousergon_lib.locks import LockHeldByAnotherWriterError, LockHolder

        monkeypatch.setenv(
            "ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED", "true"
        )
        holder = LockHolder(
            writer_id="other-writer",
            started_at="2026-05-27T12:00:00Z",
            ttl_epoch=9_999_999_999,
            hostname="other-host",
            pid=999,
        )
        with patch(
            "nousergon_lib.locks.universe_writer_lock"
        ) as mock_lock:
            mock_lock.side_effect = LockHeldByAnotherWriterError(
                holder, "s3://alpha-engine-research/locks/universe-writer.lock"
            )
            with pytest.raises(LockHeldByAnotherWriterError) as excinfo:
                daily_append_module.daily_append(date_str="2026-05-27")
        assert excinfo.value.holder.writer_id == "other-writer"
        stub_impl.assert_not_called()


# ── Helpers ──────────────────────────────────────────────────────────────


class TestHelpers:
    def test_writer_lock_enabled_dry_run_bypass(self):
        # dry_run trumps even a truthy env var
        with patch.dict(
            os.environ,
            {"ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED": "true"},
        ):
            assert (
                daily_append_module._writer_lock_enabled(dry_run=True)
                is False
            )
            assert (
                daily_append_module._writer_lock_enabled(dry_run=False)
                is True
            )

    def test_writer_lock_enabled_env_unset(self, clear_lock_env):
        assert (
            daily_append_module._writer_lock_enabled(dry_run=False)
            is False
        )

    def test_build_writer_id_shape(self, monkeypatch):
        monkeypatch.setenv("USER", "alice")
        wid = daily_append_module._build_writer_id()
        assert wid.startswith("daily_append-alice-pid")
        # pid is an int suffix
        pid_str = wid.rsplit("pid", 1)[1]
        assert pid_str.isdigit()

    def test_build_writer_id_handles_missing_user(self, monkeypatch):
        monkeypatch.delenv("USER", raising=False)
        wid = daily_append_module._build_writer_id()
        assert "daily_append-unknown-pid" in wid
