"""sparkparse — parse Spark event logs and Capture API results.

The base install (polars, pydantic, typer) can parse event logs and analyze
results without PySpark or the dashboard dependencies. PySpark is imported
lazily inside the capture paths that actually need a live session, so
``import sparkparse`` and the parsing/analysis path never require it.
"""

from sparkparse.capture import SparkparseCapture, capture, capture_context
from sparkparse.connect import ConnectSupport, probe_connect_support
from sparkparse.models import (
    CapabilityStatus,
    CaptureCapabilities,
    CaptureCapability,
    CaptureDiagnostic,
    CaptureMetadata,
    CaptureResult,
    CaptureStatus,
)

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
