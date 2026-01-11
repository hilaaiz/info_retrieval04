# temporal_retrieval.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, List, Optional, Tuple
from dateutil.relativedelta import relativedelta
from scripts.retrieval.retriever import extract_year_from_query


# -----------------------------
# Helpers: generic field access
# -----------------------------
def _get_field(chunk: Any, key: str, default=None):
    """
    Supports both:
      - dict chunks: chunk[key]
      - object chunks: getattr(chunk, key)
      - object chunks with meta: chunk.meta[key]
    """
    if isinstance(chunk, dict):
        return chunk.get(key, default)

    # object meta
    meta = getattr(chunk, "meta", None)
    if isinstance(meta, dict) and key in meta:
        return meta.get(key, default)

    # direct attribute
    return getattr(chunk, key, default)


# -----------------------------
# Helpers: timestamp extraction
# -----------------------------
def _get_ts_dt(chunk: Any) -> Optional[datetime]:
    """
    Extract datetime from a chunk.
    Supports:
      - timestamp_unix (int seconds)
      - timestamp_iso (ISO string or datetime)
    Works for dict chunks and object chunks.
    """
    ts_unix = _get_field(chunk, "timestamp_unix", None)
      - timestamp_unix (int seconds)
      - timestamp_iso (ISO string or datetime)
    Works for dict chunks and object chunks.
    """
    ts_unix = _get_field(chunk, "timestamp_unix", None)
    if ts_unix is not None:
        try:
            return datetime.utcfromtimestamp(int(ts_unix))
        except Exception:
            pass

    ts_iso = _get_field(chunk, "timestamp_iso", None)
    ts_iso = _get_field(chunk, "timestamp_iso", None)

    if isinstance(ts_iso, datetime):
        return ts_iso
    if isinstance(ts_iso, datetime):
        return ts_iso

    if isinstance(ts_iso, str):
        try:
            return datetime.fromisoformat(ts_iso)
        except ValueError:
            # handle 'Z'
            # handle 'Z'
            try:
                return datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
            except Exception:
                return None

    return None


def _sort_by_time(chunks: List[Any], newest_first: bool) -> List[Any]:
    """   
    def key_fn(c: Any):
        dt = _get_ts_dt(c)
        return (dt is None, dt if dt is not None else datetime.min)
    chunks_sorted = sorted(chunks, key=key_fn, reverse=newest_first)

    # Ensure None timestamps always last

    # Ensure None timestamps always last
    with_ts = [c for c in chunks_sorted if _get_ts_dt(c) is not None]
    no_ts = [c for c in chunks_sorted if _get_ts_dt(c) is None]
    return with_ts + no_ts


# -----------------------------
# Type filtering (4 configurations)
# -----------------------------
def filter_by_type(
    all_chunks: Iterable[Any],
    chunking_method: Optional[str] = None,
    embedding_method: Optional[str] = None,
) -> List[Any]:
    """
    Filter chunks by:
      - chunking_method: "fixed_660" / "hierarchical"
      - embedding_method: "bm25" / "dense_e5_base" (or whatever you used)
    If a filter is None => don't filter by it.
    """
    out = []
    for c in all_chunks:
        cm = _get_field(c, "chunking_method", None)
        em = _get_field(c, "embedding_method", None)

        if chunking_method is not None and cm != chunking_method:
            continue
        if embedding_method is not None and em != embedding_method:
            continue
        out.append(c)
    return out


# -----------------------------
# Core: temporal split windows
# Type filtering (4 configurations)
# -----------------------------
def filter_by_type(
    all_chunks: Iterable[Any],
    chunking_method: Optional[str] = None,
    embedding_method: Optional[str] = None,
) -> List[Any]:
    """
    Filter chunks by:
      - chunking_method: "fixed_660" / "hierarchical"
      - embedding_method: "bm25" / "dense_e5_base" (or whatever you used)
    If a filter is None => don't filter by it.
    """
    out = []
    for c in all_chunks:
        cm = _get_field(c, "chunking_method", None)
        em = _get_field(c, "embedding_method", None)

        if chunking_method is not None and cm != chunking_method:
            continue
        if embedding_method is not None and em != embedding_method:
            continue
        out.append(c)
    return out


# -----------------------------
# Core: temporal split windows
# -----------------------------
@dataclass(frozen=True)
class TemporalWindows:
    early_start: datetime
    early_end: datetime
    late_start: datetime
    late_end: datetime


def build_windows_from_corpus(all_chunks: Iterable[Any], months: int = 8) -> TemporalWindows:
def build_windows_from_corpus(all_chunks: Iterable[Any], months: int = 8) -> TemporalWindows:
    """
    Builds two windows:
      - early: [min_ts, min_ts + months]
      - early: [min_ts, min_ts + months]
      - late:  [max_ts - months, max_ts]
    """
    dts = [_get_ts_dt(c) for c in all_chunks]
    dts = [dt for dt in dts if dt is not None]

    if not dts:
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
    k: int = 10,
    months: int = 14,
    chunking_method: Optional[str] = None,
    embedding_method: Optional[str] = None,
) -> Tuple[List[Any], List[Any]]:
    """
    Returns:
      early_chunks, late_chunks

    Optional config filters (for 4 systems):
      - chunking_method: fixed_660 / hierarchical
      - embedding_method: bm25 / dense_e5_base

    Optional config filters (for 4 systems):
      - chunking_method: fixed_660 / hierarchical
      - embedding_method: bm25 / dense_e5_base

    Requirements:
      - early list sorted OLD -> NEW
      - late list sorted NEW -> OLD
      - both lists contain only chunks from the correct time window
    """
   # 0) Filter by system type (optional)
    typed_chunks = filter_by_type(
        all_chunks,
        chunking_method=chunking_method,
        embedding_method=embedding_method,
    )

    if not typed_chunks:
        return [], []

    # 0.5) Hard year filter (if year in query)
    year = extract_year_from_query(query)
    if year is not None:
        typed_chunks = [
            c for c in typed_chunks
            if c.get("timestamp_iso", "").startswith(str(year))
        ]

    if not typed_chunks:
        return [], []


    if year is not None:
        early_corpus = typed_chunks
        late_corpus = typed_chunks
    else:
        windows = build_windows_from_corpus(typed_chunks, months=months)
        early_corpus = filter_by_window(typed_chunks, windows.early_start, windows.early_end)
        late_corpus = filter_by_window(typed_chunks, windows.late_start, windows.late_end)

    # 3) Dual retrieval
    # 3) Dual retrieval
    early_top = retriever(query, early_corpus, k)
    late_top = retriever(query, late_corpus, k)

    # 4) Required ordering
    early_top = _sort_by_time(list(early_top), newest_first=False)  # old -> new
    late_top = _sort_by_time(list(late_top), newest_first=True)     # new -> old
    # 4) Required ordering
    early_top = _sort_by_time(list(early_top), newest_first=False)  # old -> new
    late_top = _sort_by_time(list(late_top), newest_first=True)     # new -> old

    return early_top, late_top



