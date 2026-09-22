"""data_quality — declared known-defect windows over published artifacts.

See :mod:`data_quality.windows` for what belongs in the register and why it
lives in code rather than in the backlog.
"""

from data_quality.windows import (  # noqa: F401
    DISPOSITIONS,
    WINDOWS,
    QualityWindow,
    overlapping,
    window_by_id,
)

__all__ = [
    "DISPOSITIONS",
    "WINDOWS",
    "QualityWindow",
    "overlapping",
    "window_by_id",
]
