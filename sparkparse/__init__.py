"""sparkparse — parse Spark event logs and Capture API results.

The base install (polars, pydantic, typer) can parse event logs and analyze
results without PySpark or the dashboard dependencies. ``SparkparseCapture``
and the Connect adapter are imported lazily so ``import sparkparse`` and the
parsing/analysis path never require PySpark.
"""

import importlib
from typing import TYPE_CHECKING, Any

from sparkparse.models import (
    CapabilityStatus,
    CaptureCapabilities,
    CaptureCapability,
    CaptureDiagnostic,
    CaptureMetadata,
    CaptureResult,
    CaptureStatus,
)

if TYPE_CHECKING:
    from sparkparse.capture import (
        SparkparseCapture,
        capture,
        capture_context,
    )
    from sparkparse.connect import ConnectSupport, probe_connect_support

__all__ = [
    "CaptureCapabilities",
    "CaptureCapability",
    "CaptureDiagnostic",
    "CaptureMetadata",
    "CaptureResult",
    "CaptureStatus",
    "CapabilityStatus",
    "ConnectSupport",
    "SparkparseCapture",
    "capture",
    "capture_context",
    "probe_connect_support",
]

_CAPTURE_NAMES = {"SparkparseCapture", "capture", "capture_context"}
_CONNECT_NAMES = {"ConnectSupport", "probe_connect_support"}


def __getattr__(name: str) -> Any:
    if name in _CAPTURE_NAMES:
        module = importlib.import_module("sparkparse.capture")
        return getattr(module, name)
    if name in _CONNECT_NAMES:
        module = importlib.import_module("sparkparse.connect")
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
