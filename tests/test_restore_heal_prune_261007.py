"""The 10-07 heal-prune restore removes only heal-window markers that hide an object.

Tracked as alpha-engine-config-I12115. The script runs once, by hand, under an
admin profile, and issues versioned deletes, so these pin what it may select.
"""

from __future__ import annotations

import datetime as dt

from scripts import restore_heal_prune_261007 as restore

UTC = dt.UTC
IN_WINDOW = dt.datetime(2026, 10, 7, 9, 50, tzinfo=UTC)
BEFORE = dt.datetime(2026, 10, 6, 9, 50, tzinfo=UTC)
AFTER = dt.datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
P = restore.LIBRARY_PREFIXES[1]


class FakeS3:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []
        self.deleted = []

    def list_object_versions(self, **kw):
        self.calls.append(kw)
        return self.pages[len(self.calls) - 1]

    def delete_objects(self, Bucket, Delete):
        self.deleted.extend(Delete["Objects"])
        return {}


def _marker(key, vid, when, latest=True):
    return {"Key": key, "VersionId": vid, "LastModified": when, "IsLatest": latest}


def _version(key, vid):
    return {
        "Key": key,
        "VersionId": vid,
        "LastModified": BEFORE,
        "IsLatest": False,
        "Size": 10,
    }


def _pages():
    return [
        {
            "DeleteMarkers": [
                _marker(P + "tdata/a", "m1", IN_WINDOW),
                _marker(P + "cstats/never-existed", "m2", IN_WINDOW),
            ],
            "Versions": [_version(P + "tdata/a", "o1")],
            "IsTruncated": True,
            "NextKeyMarker": "k",
            "NextVersionIdMarker": "v",
        },
        {
            "DeleteMarkers": [
                _marker(P + "tindex/b", "m3", IN_WINDOW),
                _marker(P + "tindex/old", "m4", BEFORE),
                _marker(P + "tindex/later", "m5", AFTER),
                _marker(P + "tindex/superseded", "m6", IN_WINDOW, latest=False),
            ],
            "Versions": [
                _version(P + "tindex/b", "o3"),
                _version(P + "tindex/old", "o4"),
                _version(P + "tindex/later", "o5"),
                _version(P + "tindex/superseded", "o6"),
            ],
            "IsTruncated": False,
        },
    ]


def test_selects_only_latest_heal_window_markers_that_hide_an_object():
    s3 = FakeS3(_pages())
    in_window, plan = restore.plan_for_prefix(
        s3, "b", P, restore.WINDOW_START, restore.WINDOW_END
    )
    assert in_window == 3
    assert plan == [(P + "tdata/a", "m1"), (P + "tindex/b", "m3")]
    assert s3.calls[1]["KeyMarker"] == "k" and s3.calls[1]["VersionIdMarker"] == "v"


def test_remove_deletes_marker_version_ids_never_object_versions():
    s3 = FakeS3(_pages())
    _, plan = restore.plan_for_prefix(
        s3, "b", P, restore.WINDOW_START, restore.WINDOW_END
    )
    assert restore.remove_markers(s3, "b", plan) == 2
    assert {o["VersionId"] for o in s3.deleted} == {"m1", "m3"}


def test_dry_run_deletes_nothing(capsys):
    class AllPrefixes(FakeS3):
        def list_object_versions(self, **kw):
            return {
                "DeleteMarkers": [_marker(kw["Prefix"] + "tdata/a", "m1", IN_WINDOW)],
                "Versions": [_version(kw["Prefix"] + "tdata/a", "o1")],
                "IsTruncated": False,
            }

    s3 = AllPrefixes([])
    assert restore.main([], s3=s3) == 0
    assert s3.deleted == []
    assert "dry_run=True" in capsys.readouterr().out
