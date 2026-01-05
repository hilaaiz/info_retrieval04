# temporal_retrieval.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, List, Optional, Tuple
from dateutil.relativedelta import relativedelta


# -----------------------------
# Helpers: timestamp extraction
# -----------------------------
def _get_ts_dt(chunk: Any) -> Optional[datetime]:
    """
    Extract datetime from a chunk.
    Supports:
      - chunk.meta["timestamp_iso"] as ISO string "YYYY-MM-DD" (or full ISO)
      - chunk.meta["timestamp_unix"] as int seconds
      - chunk.timestamp_iso directly (if you stored it that way)
    """
    meta = getattr(chunk, "meta", {}) or {}

    # 1) unix
    ts_unix = meta.get("timestamp_unix", None)
    if ts_unix is not None:
        try:
            return datetime.utcfromtimestamp(int(ts_unix))
        except Exception:
            pass

    # 2) iso in meta
    ts_iso = meta.get("timestamp_iso", None)

    # 3) iso as attribute fallback
    if ts_iso is None:
        ts_iso = getattr(chunk, "timestamp_iso", None)

    if isinstance(ts_iso, str):
        try:
            return datetime.fromisoformat(ts_iso)
        except ValueError:
            # Some ISO strings might include 'Z'
            try:
                return datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
            except Exception:
                return None

    if isinstance(ts_iso, datetime):
        return ts_iso

    return None


def _sort_by_time(chunks: List[Any], newest_first: bool) -> List[Any]:
    """
    Sort chunks by timestamp; chunks without timestamp go to the end.
    """
    def key_fn(c: Any):
        dt = _get_ts_dt(c)
        # None timestamps should be last
        return (dt is None, dt if dt is not None else datetime.min)

    chunks_sorted = sorted(chunks, key=key_fn, reverse=newest_first)
    # But reverse=True also reverses the "(dt is None)" flag; we want None always last.
    # So do a stable 2-stage sort:
    with_ts = [c for c in chunks_sorted if _get_ts_dt(c) is not None]
    no_ts = [c for c in chunks_sorted if _get_ts_dt(c) is None]
    return with_ts + no_ts


# -----------------------------
# Core: temporal split
# -----------------------------
@dataclass(frozen=True)
class TemporalWindows:
    early_start: datetime
    early_end: datetime
    late_start: datetime
    late_end: datetime


def build_windows_from_corpus(
    all_chunks: Iterable[Any],
    months: int = 8,
) -> TemporalWindows:
    """
    Builds two windows:
      - early: [min_ts, min_ts + months)
      - late:  [max_ts - months, max_ts]
    """
    dts = [_get_ts_dt(c) for c in all_chunks]
    dts = [dt for dt in dts if dt is not None]

    if not dts:
        # Fallback (no timestamps): make both windows "now"
        now = datetime.utcnow()
        return TemporalWindows(now, now, now, now)

    min_ts = min(dts)
    max_ts = max(dts)

    early_start = min_ts
    early_end = min_ts + relativedelta(months=months)

    late_end = max_ts
    late_start = max_ts - relativedelta(months=months)

    return TemporalWindows(early_start, early_end, late_start, late_end)


def filter_by_window(all_chunks: Iterable[Any], start: datetime, end: datetime) -> List[Any]:
    """
    Keep only chunks with timestamps in [start, end].
    """
    out = []
    for c in all_chunks:
        dt = _get_ts_dt(c)
        if dt is None:
            continue
        if start <= dt <= end:
            out.append(c)
    return out


# -----------------------------
# API: temporal_retrieve
# -----------------------------
RetrieverFn = Callable[[str, List[Any], int], List[Any]]

def temporal_retrieve(
    query: str,
    all_chunks: List[Any],
    retriever: RetrieverFn,
    k: int = 5,
    months: int = 8,
) -> Tuple[List[Any], List[Any]]:
    """
    Returns:
      early_chunks, late_chunks

    Requirements:
      - early list sorted OLD -> NEW
      - late list sorted NEW -> OLD
      - both lists contain only chunks from the correct time window
    """
    windows = build_windows_from_corpus(all_chunks, months=months)

    early_corpus = filter_by_window(all_chunks, windows.early_start, windows.early_end)
    late_corpus = filter_by_window(all_chunks, windows.late_start, windows.late_end)

    # retrieval
    early_top = retriever(query, early_corpus, k)
    late_top = retriever(query, late_corpus, k)

    # required ordering:
    # early: old -> new
    early_top = _sort_by_time(list(early_top), newest_first=False)
    # late: new -> old
    late_top = _sort_by_time(list(late_top), newest_first=True)

    return early_top, late_top
