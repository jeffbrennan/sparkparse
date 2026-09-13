"""Databricks collection transport.

This module owns the single boundary between sparkparse and the Databricks CLI.
Every remote call goes through :class:`DatabricksCLI`, which uses argument
arrays and structured JSON only (never shell interpolation or table parsing).
Normalization into report models lives in :mod:`sparkparse.runreport` so it can
be tested without a CLI process.

The generated CLI does not expose filter flags for Query History listing in
tested versions, so those calls go through ``databricks api get`` with URL
encoded query parameters.
"""

from __future__ import annotations

import datetime
import json
import logging
import subprocess
import time
import urllib.parse
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from sparkparse.models import (
    CollectionStatus,
    ReportDiagnostic,
    WorkflowCollectionLimits,
)

logger = logging.getLogger(__name__)

DEFAULT_LIMITS = WorkflowCollectionLimits(
    deadline_seconds=120.0,
    max_pages=20,
    max_requests=60,
    max_retries=3,
    retry_backoff_seconds=1.0,
    outputs="failed",
)

_QUERY_HISTORY_PATH = "/api/2.0/sql/history/queries"
_JOBS_RUNS_GET_PATH = "/api/2.1/jobs/runs/get"
_JOBS_RUNS_LIST_PATH = "/api/2.1/jobs/runs/list"

_RATE_LIMIT_MARKERS = (
    "429",
    "rate limit",
    "too many requests",
    "resource_exhausted",
    "retry after",
)
_AUTH_MARKERS = (
    "invalid access token",
    "invalid refresh token",
    "authentication",
    "unauthorized",
    "401",
    "403",
    "no valid credential",
)

Runner = Callable[[Sequence[str], float], "subprocess.CompletedProcess[str]"]


class DatabricksError(Exception):
    """A collection failure with a stable code for the report and CLI."""

    def __init__(self, message: str, *, code: str = "collection_error") -> None:
        super().__init__(message)
        self.code = code


class CollectionDeadlineExceeded(DatabricksError):
    def __init__(self, message: str = "collection deadline exceeded") -> None:
        super().__init__(message, code="deadline_exceeded")


def _default_runner(
    args: Sequence[str], timeout: float
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(args), capture_output=True, text=True, timeout=timeout, check=False
    )


def _now_ms() -> int:
    return int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)


class _Budget:
    """Tracks a single collection's deadline and request/page counts."""

    def __init__(self, limits: WorkflowCollectionLimits) -> None:
        self.limits = limits
        self.requests = 0
        self.pages = 0
        self.retries = 0
        self.deadline = time.monotonic() + limits.deadline_seconds
        self.exhausted = False
        self.reason: str | None = None

    def remaining(self) -> float:
        return self.deadline - time.monotonic()

    def take_request(self) -> float:
        if self.remaining() <= 0:
            self.exhausted = True
            self.reason = "deadline"
            raise CollectionDeadlineExceeded()
        if self.requests >= self.limits.max_requests:
            self.exhausted = True
            self.reason = "max_requests"
            raise DatabricksError(
                f"request budget exhausted ({self.limits.max_requests})",
                code="budget_exhausted",
            )
        self.requests += 1
        return max(self.remaining(), 0.1)

    def take_page(self) -> None:
        if self.pages >= self.limits.max_pages:
            self.exhausted = True
            self.reason = "max_pages"
            raise DatabricksError(
                f"page budget exhausted ({self.limits.max_pages})",
                code="budget_exhausted",
            )
        self.pages += 1


class DatabricksCLI:
    """Small, injectable client over the Databricks CLI."""

    def __init__(
        self,
        profile: str | None = None,
        *,
        cli_path: str = "databricks",
        runner: Runner | None = None,
        limits: WorkflowCollectionLimits | None = None,
    ) -> None:
        self.profile = profile
        self.cli_path = cli_path
        self.runner = runner or _default_runner
        self.limits = limits or DEFAULT_LIMITS
        self.budget = _Budget(self.limits)

    def _base_args(self, args: Sequence[str]) -> list[str]:
        command = [self.cli_path, *args]
        if self.profile is not None:
            command += ["--profile", self.profile]
        command += ["--output", "json"]
        return command

    def _run_json(self, args: Sequence[str]) -> Any:
        while True:
            timeout = self.budget.take_request()
            command = self._base_args(args)
            logger.debug("databricks call: %s", command)
            try:
                proc = self.runner(command, timeout)
            except subprocess.TimeoutExpired as exc:
                raise CollectionDeadlineExceeded() from exc
            if proc.returncode == 0:
                return self._parse_stdout(proc.stdout, args)
            stderr = (proc.stderr or "").strip()
            if (
                self._is_rate_limit(stderr)
                and self.budget.retries < self.limits.max_retries
            ):
                self.budget.retries += 1
                delay = self.limits.retry_backoff_seconds * (
                    2 ** (self.budget.retries - 1)
                )
                time.sleep(min(delay, max(self.budget.remaining(), 0.0)))
                continue
            raise self._error_from_stderr(stderr)

    @staticmethod
    def _parse_stdout(stdout: str, args: Sequence[str]) -> Any:
        text = stdout.strip()
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise DatabricksError(
                f"Databricks CLI returned non-JSON output for {' '.join(args)}",
                code="invalid_json",
            ) from exc

    @staticmethod
    def _is_rate_limit(stderr: str) -> bool:
        lowered = stderr.lower()
        return any(marker in lowered for marker in _RATE_LIMIT_MARKERS)

    @classmethod
    def _error_from_stderr(cls, stderr: str) -> DatabricksError:
        lowered = stderr.lower()
        if any(marker in lowered for marker in _AUTH_MARKERS):
            return DatabricksError(
                f"Databricks authentication failed: {stderr}",
                code="auth_error",
            )
        return DatabricksError(stderr or "Databricks CLI call failed")

    def jobs_get_run(
        self, run_id: str, *, page_token: str | None = None
    ) -> dict[str, Any]:
        query = [
            f"run_id={urllib.parse.quote(str(run_id))}",
            "include_resolved_values=true",
        ]
        if page_token:
            query.append(f"page_token={urllib.parse.quote(page_token)}")
        raw = self._run_json(["api", "get", f"{_JOBS_RUNS_GET_PATH}?{'&'.join(query)}"])
        assert isinstance(raw, dict)
        return raw

    def jobs_list_runs(
        self,
        *,
        job_id: str | None = None,
        page_token: str | None = None,
        limit: int = 25,
        active_only: bool | None = None,
    ) -> dict[str, Any]:
        query = [f"limit={limit}"]
        if job_id is not None:
            query.append(f"job_id={urllib.parse.quote(str(job_id))}")
        if active_only is not None:
            query.append(f"active_only={'true' if active_only else 'false'}")
        if page_token:
            query.append(f"page_token={urllib.parse.quote(page_token)}")
        raw = self._run_json(
            ["api", "get", f"{_JOBS_RUNS_LIST_PATH}?{'&'.join(query)}"]
        )
        assert isinstance(raw, dict)
        return raw

    def run_output(self, run_id: str) -> dict[str, Any]:
        raw = self._run_json(["jobs", "get-run-output", str(run_id)])
        assert isinstance(raw, dict)
        return raw

    def query_history_page(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int,
        max_results: int = 100,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        query = [
            "include_metrics=true",
            f"max_results={max_results}",
            f"filter_by.query_start_time_range.start_time_ms={start_time_ms}",
            f"filter_by.query_start_time_range.end_time_ms={end_time_ms}",
        ]
        if page_token:
            query.append(f"page_token={urllib.parse.quote(page_token)}")
        raw = self._run_json(["api", "get", f"{_QUERY_HISTORY_PATH}?{'&'.join(query)}"])
        assert isinstance(raw, dict)
        return raw

    def cluster_get(self, cluster_id: str) -> dict[str, Any]:
        raw = self._run_json(
            ["api", "get", f"/api/2.1/clusters/get?cluster_id={cluster_id}"]
        )
        assert isinstance(raw, dict)
        return raw


@dataclass
class RawRunCollection:
    """Raw API responses plus collection bookkeeping, before normalization."""

    run: dict[str, Any]
    tasks: list[dict[str, Any]]
    queries: list[dict[str, Any]]
    cluster_lookups: dict[str, dict[str, Any]] = field(default_factory=dict)
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    diagnostics: list[ReportDiagnostic] = field(default_factory=list)
    collection_status: CollectionStatus = CollectionStatus.complete
    query_discovery_complete: bool = True
    query_denied: bool = False
    discovered_query_count: int = 0
    request_count: int = 0
    page_count: int = 0
    collection_time_ms: int | None = None
    limits: WorkflowCollectionLimits = field(default_factory=lambda: DEFAULT_LIMITS)


def _merge_array(target: dict[str, Any], page: dict[str, Any]) -> None:
    """Merge a paginated get-run page into the first page's array properties."""
    for key, value in page.items():
        if isinstance(value, list):
            existing = target.get(key)
            if isinstance(existing, list):
                existing.extend(value)
            else:
                target[key] = value
        elif key not in target:
            target[key] = value


def fetch_full_run(cli: DatabricksCLI, run_id: str) -> tuple[dict[str, Any], bool]:
    """Fetch a run, following page tokens for paginated array properties.

    Returns the merged run plus whether every page was retrieved. Budget or
    transport exhaustion stops pagination but keeps the pages already merged,
    so a partial run is never discarded.
    """
    run = cli.jobs_get_run(run_id)
    token = run.get("next_page_token")
    complete = True
    while token:
        try:
            cli.budget.take_page()
            page = cli.jobs_get_run(run_id, page_token=token)
        except DatabricksError:
            complete = False
            break
        _merge_array(run, page)
        token = page.get("next_page_token")
    return run, complete


def resolve_run_id(cli: DatabricksCLI, job_id: str) -> tuple[str, bool]:
    """Return the latest run for a job and whether it is still active.

    Runs are returned newest first, so the first terminal run encountered is the
    latest terminal run. Pagination continues past a page of active runs until a
    terminal run is found or the page budget is exhausted.
    """
    token: str | None = None
    fallback: dict[str, Any] | None = None
    while True:
        try:
            cli.budget.take_page()
            response = cli.jobs_list_runs(job_id=job_id, page_token=token, limit=25)
        except DatabricksError:
            break
        runs = response.get("runs") or []
        for run in runs:
            if fallback is None:
                fallback = run
            state = (run.get("state") or {}).get("life_cycle_state")
            if state not in (None, "RUNNING", "PENDING", "QUEUED", "BLOCKED"):
                return str(run["run_id"]), False
        token = response.get("next_page_token")
        if not token:
            break
    if fallback is None:
        raise DatabricksError(f"job {job_id} has no retained runs", code="no_runs")
    return str(fallback["run_id"]), True


def _task_failure(task: dict[str, Any]) -> bool:
    result_state = (task.get("state") or {}).get("result_state")
    return result_state in ("FAILED", "TIMEDOUT", "CANCELED", "UPSTREAM_FAILED")


def collect_run(
    cli: DatabricksCLI,
    *,
    run_id: str | None = None,
    job_id: str | None = None,
    outputs: str = "failed",
    max_outputs: int = 5,
) -> RawRunCollection:
    """Collect a run's metadata, tasks, compute, outputs and query metrics.

    Network failures raise :class:`DatabricksError`; bounded exhaustion is
    reported as a partial collection rather than raising, so a useful report is
    still produced.
    """
    started = _now_ms()
    active = False
    if run_id is None:
        assert job_id is not None
        run_id, active = resolve_run_id(cli, job_id)

    diagnostics: list[ReportDiagnostic] = []
    status = CollectionStatus.complete

    run, run_complete = fetch_full_run(cli, run_id)
    if not run_complete:
        diagnostics.append(
            ReportDiagnostic(
                code="run_pagination_incomplete",
                message="run metadata pagination stopped early; task list may be partial",
                severity="warning",
            )
        )
    parent_run_id = run.get("job_run_id")
    if parent_run_id is not None and str(parent_run_id) != str(run.get("run_id")):
        raise DatabricksError(
            f"run {run.get('run_id')} is a task run; its parent job run is "
            f"{parent_run_id}. Pass the parent job-run ID with --run-id.",
            code="task_run_id",
        )
    life_cycle = (run.get("state") or {}).get("life_cycle_state")
    if life_cycle in ("RUNNING", "PENDING", "QUEUED", "BLOCKED"):
        active = True
    tasks = list(run.get("tasks") or [])

    # Compute snapshots embedded in the run; only missing classic clusters
    # trigger a lookup, which is labelled as a current observation by the
    # normalizer. Distinct lookups are cached.
    cluster_lookups: dict[str, dict[str, Any]] = {}
    for task in tasks:
        cluster_id = task.get("existing_cluster_id")
        if not cluster_id or cluster_id in cluster_lookups:
            continue
        try:
            cluster_lookups[str(cluster_id)] = cli.cluster_get(str(cluster_id))
        except DatabricksError as exc:
            diagnostics.append(
                ReportDiagnostic(
                    code="cluster_lookup_failed",
                    message=str(exc),
                    scope=str(cluster_id),
                )
            )

    # Task output is enrichment, not the timing source. Retrieve bounded output
    # for failed tasks by default, or every task with ``--outputs all``.
    outputs_by_run: dict[str, dict[str, Any]] = {}
    if outputs != "none":
        wanted = (
            [t for t in tasks if _task_failure(t)] if outputs == "failed" else tasks
        )
        for task in wanted[:max_outputs]:
            task_run_id = task.get("run_id")
            if task_run_id is None:
                continue
            try:
                outputs_by_run[str(task_run_id)] = cli.run_output(str(task_run_id))
            except DatabricksError as exc:
                diagnostics.append(
                    ReportDiagnostic(
                        code="output_unavailable",
                        message=str(exc),
                        scope=task.get("task_key"),
                    )
                )
        if len(wanted) > max_outputs:
            diagnostics.append(
                ReportDiagnostic(
                    code="outputs_truncated",
                    message=(
                        f"{len(wanted) - max_outputs} task output(s) not retrieved "
                        f"(bounded at {max_outputs})"
                    ),
                )
            )

    queries: list[dict[str, Any]] = []
    query_discovery_complete = True
    query_denied = False
    discovered_query_count = 0
    start_ms = run.get("start_time")
    end_ms = run.get("end_time") or _now_ms()
    if start_ms is not None:
        (
            queries,
            query_discovery_complete,
            discovered_query_count,
            query_error,
        ) = _collect_queries(cli, start_time_ms=int(start_ms), end_time_ms=int(end_ms))
        if query_error is not None:
            query_denied = query_error.code == "auth_error"
            diagnostics.append(
                ReportDiagnostic(
                    code="query_history_unavailable",
                    message=str(query_error),
                    severity="warning",
                )
            )
            query_discovery_complete = False
    if active:
        status = CollectionStatus.partial
        diagnostics.append(
            ReportDiagnostic(
                code="active_run",
                message="run is still active; report is a labeled partial snapshot",
                severity="info",
            )
        )
    if not run_complete or not query_discovery_complete:
        status = CollectionStatus.partial
    if cli.budget.exhausted:
        status = CollectionStatus.partial
        diagnostics.append(
            ReportDiagnostic(
                code="collection_budget_exhausted",
                message=f"collection stopped early ({cli.budget.reason})",
                severity="warning",
            )
        )

    return RawRunCollection(
        run=run,
        tasks=tasks,
        queries=queries,
        cluster_lookups=cluster_lookups,
        outputs=outputs_by_run,
        diagnostics=diagnostics,
        collection_status=status,
        query_discovery_complete=query_discovery_complete,
        query_denied=query_denied,
        discovered_query_count=discovered_query_count,
        request_count=cli.budget.requests,
        page_count=cli.budget.pages,
        collection_time_ms=_now_ms() - started,
        limits=cli.limits,
    )


def _collect_queries(
    cli: DatabricksCLI, *, start_time_ms: int, end_time_ms: int
) -> tuple[list[dict[str, Any]], bool, int, DatabricksError | None]:
    """Page through Query History, returning everything collected so far.

    A later-page failure is reported through the returned error while the pages
    already fetched are preserved, so a useful partial snapshot survives budget
    exhaustion or a transport error.
    """
    queries: list[dict[str, Any]] = []
    token: str | None = None
    complete = True
    discovered = 0
    error: DatabricksError | None = None
    while True:
        try:
            cli.budget.take_page()
            page = cli.query_history_page(
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                page_token=token,
            )
        except DatabricksError as exc:
            complete = False
            error = exc
            break
        results = page.get("res") or []
        discovered += len(results)
        queries.extend(results)
        token = page.get("next_page_token")
        if not page.get("has_next_page"):
            break
        if not token:
            complete = False
            break
    return queries, complete, discovered, error
