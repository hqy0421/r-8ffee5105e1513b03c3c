"""
CDC dimension backfill.

Consumes already-grouped native transactions (row events read from
:class:`~pymysqlreplication.BinLogStreamReader`) together with the binlog
file/pos at the end of each transaction, and maintains a SQLite-backed
slowly-changing-dimension (SCD2, half-open interval) model:

* ``cdc_raw_points`` - every materialized dimension change point keyed by
  ``pointID``.  Same-value points are all preserved here, even when the
  dimension intervals they produce get merged.
* ``cdc_dim`` - half-open SCD2 intervals ``[valid_from, valid_to)`` keyed by
  ``entityKey`` / effective time, with a stable surrogate ``dim_id``.
* ``cdc_fact`` - facts keyed by ``factID`` whose ``dim_id`` reference is
  re-linked by event time; every other field is left untouched.
* ``cdc_meta`` - the binlog checkpoint (file/pos) and bookkeeping counters.

Semantics
---------

* ``before_values`` / ``after_values`` and the ``none_sources`` map produced
  by the row events are used to tell a column that is *missing* from the
  binlog image (``binlog_row_image=MINIMAL``, source ``cols bitmap``) apart
  from a column explicitly set to ``NULL`` (source ``null``).  Missing
  columns are backfilled from the materialized state stored in
  ``cdc_raw_points`` / ``cdc_fact``; explicit NULLs overwrite it.
* Consecutive intervals with identical attributes are merged, but the raw
  points behind them are never dropped.
* Late arrivals, corrections, deletions and effective-time migrations all
  rebuild the affected ``entityKey`` intervals.  Two points sharing the same
  ``entityKey`` and effective time are rejected with
  :class:`CdcDuplicatePointError`.
* When intervals are rebuilt, an interval whose starting point (effective
  time *and* attributes) is unchanged keeps its ``dim_id``.
* Facts are re-linked by event time after every relevant change; a fact
  whose event time falls outside every interval gets ``dim_id = NULL``.
* Raw points/facts, the dimension table, the facts and the checkpoint are
  written in a single SQLite transaction: on failure everything stays as it
  was, replaying a complete transaction is idempotent (checkpoint guard),
  and a reopened database simply continues from the last checkpoint.
* Declared times and attribute values are stored in a typed canonical
  encoding so ``Decimal``, ``datetime`` (microseconds), arbitrary precision
  ints, bytes, ... round-trip without precision loss.

The existing streaming API is untouched; this module is purely additive.
"""

import base64
import datetime
import decimal
import json
import re
import sqlite3
import threading

from .constants import NONE_SOURCE

__all__ = [
    "CdcDimensionBackfill",
    "CdcDuplicatePointError",
    "CdcError",
    "group_transactions",
]

_SCHEMA_VERSION = "1"

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cdc_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cdc_raw_points (
    point_id   TEXT PRIMARY KEY,
    entity_key TEXT NOT NULL,
    eff_time   TEXT NOT NULL,
    attrs      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cdc_raw_points_entity_idx
    ON cdc_raw_points (entity_key);
CREATE TABLE IF NOT EXISTS cdc_dim (
    dim_id     INTEGER PRIMARY KEY,
    entity_key TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to   TEXT,
    attrs      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cdc_dim_entity_idx
    ON cdc_dim (entity_key);
CREATE TABLE IF NOT EXISTS cdc_fact (
    fact_id    TEXT PRIMARY KEY,
    entity_key TEXT NOT NULL,
    event_time TEXT NOT NULL,
    dim_id     INTEGER,
    fields     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cdc_fact_entity_idx
    ON cdc_fact (entity_key);
"""


class CdcError(Exception):
    """Base error raised by the CDC dimension backfill."""


class CdcDuplicatePointError(CdcError):
    """Two change points share the same entityKey and effective time."""

    def __init__(self, entity_key, effective_time, point_ids):
        self.entity_key = entity_key
        self.effective_time = effective_time
        self.point_ids = point_ids
        super().__init__(
            "duplicate dimension change points for entityKey=%r at %r: "
            "pointIDs %r" % (entity_key, effective_time, point_ids)
        )


# ---------------------------------------------------------------------------
# Typed canonical value codec (precision preserving)
# ---------------------------------------------------------------------------

_T_NULL = "null"
_T_BOOL = "bool"
_T_INT = "int"
_T_FLOAT = "float"
_T_DECIMAL = "decimal"
_T_DATETIME = "datetime"
_T_DATE = "date"
_T_TIME = "time"
_T_TIMEDELTA = "timedelta"
_T_STR = "str"
_T_BYTES = "bytes"
_T_SET = "set"
_T_LIST = "list"
_T_DICT = "dict"


def _canonical_json(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _encode_value(value):
    """Encode a Python value as a JSON-safe ``[type_tag, payload]`` pair."""
    if value is None:
        return [_T_NULL, None]
    if isinstance(value, bool):
        return [_T_BOOL, int(value)]
    if isinstance(value, int):
        return [_T_INT, str(value)]
    if isinstance(value, float):
        return [_T_FLOAT, repr(value)]
    if isinstance(value, decimal.Decimal):
        return [_T_DECIMAL, str(value)]
    if isinstance(value, datetime.datetime):
        return [_T_DATETIME, value.isoformat()]
    if isinstance(value, datetime.date):
        return [_T_DATE, value.isoformat()]
    if isinstance(value, datetime.time):
        return [_T_TIME, value.isoformat()]
    if isinstance(value, datetime.timedelta):
        return [_T_TIMEDELTA, [value.days, value.seconds, value.microseconds]]
    if isinstance(value, str):
        return [_T_STR, value]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return [_T_BYTES, base64.b64encode(bytes(value)).decode("ascii")]
    if isinstance(value, (set, frozenset)):
        items = [_encode_value(item) for item in value]
        items.sort(key=_canonical_json)
        return [_T_SET, items]
    if isinstance(value, (list, tuple)):
        return [_T_LIST, [_encode_value(item) for item in value]]
    if isinstance(value, dict):
        return [_T_DICT, {str(key): _encode_value(val) for key, val in value.items()}]
    raise CdcError("unsupported value type: %r" % type(value).__name__)


def _decode_value(encoded):
    tag, payload = encoded
    if tag == _T_NULL:
        return None
    if tag == _T_BOOL:
        return bool(payload)
    if tag == _T_INT:
        return int(payload)
    if tag == _T_FLOAT:
        return float(payload)
    if tag == _T_DECIMAL:
        return decimal.Decimal(payload)
    if tag == _T_DATETIME:
        return datetime.datetime.fromisoformat(payload)
    if tag == _T_DATE:
        return datetime.date.fromisoformat(payload)
    if tag == _T_TIME:
        return datetime.time.fromisoformat(payload)
    if tag == _T_TIMEDELTA:
        days, seconds, microseconds = payload
        return datetime.timedelta(
            days=days, seconds=seconds, microseconds=microseconds
        )
    if tag == _T_STR:
        return payload
    if tag == _T_BYTES:
        return base64.b64decode(payload.encode("ascii"))
    if tag == _T_SET:
        return set(_decode_value(item) for item in payload)
    if tag == _T_LIST:
        return [_decode_value(item) for item in payload]
    if tag == _T_DICT:
        return {key: _decode_value(item) for key, item in payload.items()}
    raise CdcError("unknown encoded type tag: %r" % (tag,))


def _sort_key(encoded):
    """Total ordering key for an encoded ``[tag, payload]`` pair."""
    tag, payload = encoded
    if tag == _T_NULL:
        return (0, 0)
    if tag == _T_BOOL:
        return (1, payload)
    if tag in (_T_INT, _T_DECIMAL):
        return (2, decimal.Decimal(payload))
    if tag == _T_FLOAT:
        number = decimal.Decimal(payload)
        return (2, 0 if number.is_nan() else number)
    if tag in (_T_DATETIME, _T_DATE):
        return (3, payload)
    if tag == _T_TIME:
        return (4, payload)
    if tag == _T_STR:
        return (5, payload)
    if tag == _T_BYTES:
        return (6, base64.b64decode(payload.encode("ascii")))
    if tag == _T_TIMEDELTA:
        days, seconds, microseconds = payload
        return (7, (days * 86400 + seconds) * 1000000 + microseconds)
    return (8, _canonical_json(encoded))


def _encode_key(value):
    """Encode a scalar key (id / entityKey / declared time) as canon. text."""
    return _canonical_json(_encode_value(value))


def _decode_key(text):
    return _decode_value(json.loads(text))


def _encode_attrs(attrs):
    return _canonical_json({key: _encode_value(val) for key, val in attrs.items()})


def _decode_attrs(text):
    return {key: _decode_value(val) for key, val in json.loads(text).items()}


# ---------------------------------------------------------------------------
# Row event helpers
# ---------------------------------------------------------------------------

_ROW_EVENT_KINDS = {
    "WriteRowsEvent": "insert",
    "UpdateRowsEvent": "update",
    "DeleteRowsEvent": "delete",
}

_MISSING = object()


def _class_names(event):
    return {klass.__name__ for klass in type(event).__mro__}


def _resolve_row_kind(event):
    """Return 'insert' / 'update' / 'delete' for a row event, else None.

    Resolved through the MRO so subclasses (e.g. PartialUpdateRowsEvent)
    and duck-typed events with the same class names are both honoured.
    """
    for klass in type(event).__mro__:
        kind = _ROW_EVENT_KINDS.get(klass.__name__)
        if kind is not None:
            return kind
    return None


def _present_values(values, none_sources):
    """Keep only the columns actually present in the binlog image.

    A column absent from the image (``binlog_row_image=MINIMAL``) decodes to
    ``None`` with ``none_sources[col] == COLS_BITMAP`` and is dropped here so
    the caller can backfill it from the materialized state; likewise a JSON
    column reported as ``JSON_PARTIAL_UPDATE`` ("same with before values").
    An explicit SQL ``NULL`` (source ``null``) stays and overwrites the
    stored value, and an ``EMPTY_SET`` marker is restored to a real empty
    set.  Any other decode note keeps the (present) value as-is.
    """
    if not none_sources:
        return dict(values)
    present = {}
    for column, value in values.items():
        source = none_sources.get(column)
        if value is None and source in (
            NONE_SOURCE.COLS_BITMAP,
            NONE_SOURCE.JSON_PARTIAL_UPDATE,
        ):
            continue
        if value is None and source == NONE_SOURCE.EMPTY_SET:
            present[column] = set()
            continue
        present[column] = value
    return present


def _event_schema_table(event):
    schema = getattr(event, "schema", None)
    table = getattr(event, "table", None)
    if isinstance(schema, bytes):
        schema = schema.decode("utf-8", "replace")
    if isinstance(table, bytes):
        table = table.decode("utf-8", "replace")
    return schema, table


def _position_key(log_file, log_pos):
    """Orderable key for a binlog position (numeric suffix aware)."""
    name = log_file or ""
    match = re.search(r"(\d+)$", name)
    if match:
        return (name[: match.start()], int(match.group(1)), int(log_pos))
    return (name, -1, int(log_pos))


# ---------------------------------------------------------------------------
# Main backfill processor
# ---------------------------------------------------------------------------


class CdcDimensionBackfill:
    """Apply grouped native transactions into a SQLite SCD2 model.

    :param db_path: path of the SQLite state database (``":memory:"`` works).
    :param point_table: source table holding the dimension change points.
    :param fact_table: source table holding the facts.
    :param point_id_column: primary key column of the point table.
    :param fact_id_column: primary key column of the fact table.
    :param entity_key_column: entityKey column shared by both tables.
    :param effective_time_column: declared effective-time column of points.
    :param event_time_column: declared event-time column of facts.
    :param schema: only process events from this schema (None = any schema).
    :param connection: optional existing ``sqlite3.Connection`` to use.
    """

    def __init__(
        self,
        db_path=None,
        point_table="point",
        fact_table="fact",
        point_id_column="pointID",
        fact_id_column="factID",
        entity_key_column="entityKey",
        effective_time_column="effective_time",
        event_time_column="event_time",
        schema=None,
        connection=None,
    ):
        if point_table == fact_table:
            raise ValueError("point_table and fact_table must differ")
        if connection is None and db_path is None:
            raise ValueError("db_path or connection is required")
        self._point_table = point_table
        self._fact_table = fact_table
        self._point_id_column = point_id_column
        self._fact_id_column = fact_id_column
        self._entity_key_column = entity_key_column
        self._effective_time_column = effective_time_column
        self._event_time_column = event_time_column
        self._schema = schema
        self._lock = threading.RLock()
        self._owns_connection = connection is None
        self._conn = connection or sqlite3.connect(db_path, isolation_level=None)
        self._conn.execute("PRAGMA busy_timeout = 5000")
        if self._owns_connection:
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA synchronous = FULL")
        self._ensure_schema()

    # -- schema / meta ------------------------------------------------------

    def _ensure_schema(self):
        with self._lock:
            self._conn.executescript(_SCHEMA_SQL)
            cur = self._conn.cursor()
            version = self._get_meta(cur, "schema_version")
            if version is None:
                self._set_meta(cur, "schema_version", _SCHEMA_VERSION)
            elif version != _SCHEMA_VERSION:
                raise CdcError(
                    "unsupported cdc schema version: %r" % (version,)
                )

    @staticmethod
    def _get_meta(cur, key):
        cur.execute("SELECT value FROM cdc_meta WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

    @staticmethod
    def _set_meta(cur, key, value):
        cur.execute(
            "INSERT OR REPLACE INTO cdc_meta (key, value) VALUES (?, ?)",
            (key, str(value)),
        )

    def _alloc_dim_id(self, cur):
        value = self._get_meta(cur, "next_dim_id")
        next_id = int(value) if value is not None else 1
        self._set_meta(cur, "next_dim_id", str(next_id + 1))
        return next_id

    def _checkpoint_row(self, cur):
        cur.execute(
            "SELECT key, value FROM cdc_meta WHERE key IN ('log_file', 'log_pos')"
        )
        rows = dict(cur.fetchall())
        if "log_file" not in rows or "log_pos" not in rows:
            return None
        return rows["log_file"], int(rows["log_pos"])

    @property
    def checkpoint(self):
        """The last applied ``(log_file, log_pos)`` or ``None``."""
        with self._lock:
            return self._checkpoint_row(self._conn.cursor())

    def close(self):
        with self._lock:
            if self._owns_connection and self._conn is not None:
                self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # -- public apply API ---------------------------------------------------

    def apply_transaction(self, events, log_file, log_pos):
        """Apply one grouped native transaction atomically.

        :param events: the row events of the transaction (other event types
            are ignored); may be any iterable.
        :param log_file: binlog file at the end of the transaction.
        :param log_pos: binlog position at the end of the transaction.
        :returns: ``True`` when applied, ``False`` when skipped because the
            position was already covered by the checkpoint (idempotent
            replay of a complete transaction).
        :raises CdcError: on inconsistent input; the database is left
            untouched (everything stays as it was).
        """
        if log_file is None or log_pos is None:
            raise ValueError("log_file and log_pos are required")
        log_pos = int(log_pos)
        with self._lock:
            cur = self._conn.cursor()
            dim_entities = set()
            fact_entities = set()
            try:
                cur.execute("BEGIN IMMEDIATE")
                checkpoint = self._checkpoint_row(cur)
                if checkpoint is not None and _position_key(
                    log_file, log_pos
                ) <= _position_key(*checkpoint):
                    cur.execute("ROLLBACK")
                    return False
                for event in events:
                    self._apply_event(cur, event, dim_entities, fact_entities)
                for entity_text in dim_entities:
                    self._rebuild_dimension(cur, entity_text)
                for entity_text in dim_entities | fact_entities:
                    self._relink_facts(cur, entity_text)
                self._set_meta(cur, "log_file", log_file)
                self._set_meta(cur, "log_pos", str(log_pos))
                cur.execute("COMMIT")
                return True
            except Exception:
                self._conn.rollback()
                raise

    # -- event dispatch -----------------------------------------------------

    def _apply_event(self, cur, event, dim_entities, fact_entities):
        kind = _resolve_row_kind(event)
        if kind is None:
            return
        schema, table = _event_schema_table(event)
        if self._schema is not None and schema != self._schema:
            return
        if table == self._point_table:
            handler = {
                "insert": self._apply_point_insert,
                "update": self._apply_point_update,
                "delete": self._apply_point_delete,
            }[kind]
            entities = dim_entities
        elif table == self._fact_table:
            handler = {
                "insert": self._apply_fact_insert,
                "update": self._apply_fact_update,
                "delete": self._apply_fact_delete,
            }[kind]
            entities = fact_entities
        else:
            return
        for row in getattr(event, "rows", None) or []:
            handler(cur, row, entities)

    # -- point table ----------------------------------------------------------

    def _split_point(self, values):
        values = dict(values)
        if self._point_id_column not in values:
            raise CdcError("point row carries no %s" % self._point_id_column)
        point_id = values.pop(self._point_id_column)
        entity_key = values.pop(self._entity_key_column, None)
        eff_time = values.pop(self._effective_time_column, None)
        return point_id, entity_key, eff_time, values

    def _load_point_values(self, cur, point_id_text):
        cur.execute(
            "SELECT entity_key, eff_time, attrs FROM cdc_raw_points"
            " WHERE point_id = ?",
            (point_id_text,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        values = _decode_attrs(row[2])
        values[self._entity_key_column] = _decode_key(row[0])
        values[self._effective_time_column] = _decode_key(row[1])
        values[self._point_id_column] = _decode_key(point_id_text)
        return values

    def _store_point(self, cur, values, touched):
        point_id, entity_key, eff_time, attrs = self._split_point(values)
        cur.execute(
            "INSERT OR REPLACE INTO cdc_raw_points"
            " (point_id, entity_key, eff_time, attrs) VALUES (?, ?, ?, ?)",
            (
                _encode_key(point_id),
                _encode_key(entity_key),
                _encode_key(eff_time),
                _encode_attrs(attrs),
            ),
        )
        touched.add(_encode_key(entity_key))

    def _apply_point_insert(self, cur, row, touched):
        values = _present_values(row.get("values") or {}, row.get("none_sources"))
        point_id = values.get(self._point_id_column, _MISSING)
        if point_id is _MISSING:
            raise CdcError("point insert row carries no %s" % self._point_id_column)
        stored = self._load_point_values(cur, _encode_key(point_id))
        if stored is not None:
            touched.add(_encode_key(stored[self._entity_key_column]))
        self._store_point(cur, values, touched)

    def _apply_point_update(self, cur, row, touched):
        before = _present_values(
            row.get("before_values") or {}, row.get("before_none_sources")
        )
        after = _present_values(
            row.get("after_values") or {}, row.get("after_none_sources")
        )
        before_id = before.get(self._point_id_column, _MISSING)
        after_id = after.get(self._point_id_column, _MISSING)
        if before_id is _MISSING and after_id is _MISSING:
            raise CdcError("point update row carries no %s" % self._point_id_column)
        pk_changed = (
            before_id is not _MISSING
            and after_id is not _MISSING
            and _encode_key(before_id) != _encode_key(after_id)
        )
        lookup_id = (
            before_id
            if pk_changed
            else (after_id if after_id is not _MISSING else before_id)
        )
        stored = self._load_point_values(cur, _encode_key(lookup_id))
        if stored is not None:
            touched.add(_encode_key(stored[self._entity_key_column]))
        base = stored if stored is not None else before
        merged = dict(base)
        merged.update(after)
        if pk_changed:
            cur.execute(
                "DELETE FROM cdc_raw_points WHERE point_id = ?",
                (_encode_key(before_id),),
            )
        self._store_point(cur, merged, touched)

    def _apply_point_delete(self, cur, row, touched):
        before = _present_values(row.get("values") or {}, row.get("none_sources"))
        point_id = before.get(self._point_id_column, _MISSING)
        if point_id is _MISSING:
            raise CdcError("point delete row carries no %s" % self._point_id_column)
        point_id_text = _encode_key(point_id)
        stored = self._load_point_values(cur, point_id_text)
        if stored is None:
            return
        touched.add(_encode_key(stored[self._entity_key_column]))
        cur.execute(
            "DELETE FROM cdc_raw_points WHERE point_id = ?", (point_id_text,)
        )

    # -- fact table -----------------------------------------------------------

    def _split_fact(self, values):
        values = dict(values)
        if self._fact_id_column not in values:
            raise CdcError("fact row carries no %s" % self._fact_id_column)
        fact_id = values.pop(self._fact_id_column)
        entity_key = values.pop(self._entity_key_column, None)
        event_time = values.pop(self._event_time_column, None)
        return fact_id, entity_key, event_time, values

    def _load_fact(self, cur, fact_id_text):
        cur.execute(
            "SELECT entity_key, event_time, dim_id, fields FROM cdc_fact"
            " WHERE fact_id = ?",
            (fact_id_text,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        values = _decode_attrs(row[3])
        values[self._entity_key_column] = _decode_key(row[0])
        values[self._event_time_column] = _decode_key(row[1])
        values[self._fact_id_column] = _decode_key(fact_id_text)
        return values, row[2]

    def _store_fact(self, cur, values, dim_id, touched):
        fact_id, entity_key, event_time, fields = self._split_fact(values)
        cur.execute(
            "INSERT OR REPLACE INTO cdc_fact"
            " (fact_id, entity_key, event_time, dim_id, fields)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                _encode_key(fact_id),
                _encode_key(entity_key),
                _encode_key(event_time),
                dim_id,
                _encode_attrs(fields),
            ),
        )
        touched.add(_encode_key(entity_key))

    def _apply_fact_insert(self, cur, row, touched):
        values = _present_values(row.get("values") or {}, row.get("none_sources"))
        fact_id = values.get(self._fact_id_column, _MISSING)
        if fact_id is _MISSING:
            raise CdcError("fact insert row carries no %s" % self._fact_id_column)
        stored = self._load_fact(cur, _encode_key(fact_id))
        if stored is not None:
            touched.add(_encode_key(stored[0][self._entity_key_column]))
        # dim_id is (re-)resolved in the re-link phase.
        self._store_fact(cur, values, None, touched)

    def _apply_fact_update(self, cur, row, touched):
        before = _present_values(
            row.get("before_values") or {}, row.get("before_none_sources")
        )
        after = _present_values(
            row.get("after_values") or {}, row.get("after_none_sources")
        )
        before_id = before.get(self._fact_id_column, _MISSING)
        after_id = after.get(self._fact_id_column, _MISSING)
        if before_id is _MISSING and after_id is _MISSING:
            raise CdcError("fact update row carries no %s" % self._fact_id_column)
        pk_changed = (
            before_id is not _MISSING
            and after_id is not _MISSING
            and _encode_key(before_id) != _encode_key(after_id)
        )
        lookup_id = (
            before_id
            if pk_changed
            else (after_id if after_id is not _MISSING else before_id)
        )
        stored = self._load_fact(cur, _encode_key(lookup_id))
        if stored is not None:
            touched.add(_encode_key(stored[0][self._entity_key_column]))
        base = stored[0] if stored is not None else before
        merged = dict(base)
        merged.update(after)
        if pk_changed:
            cur.execute(
                "DELETE FROM cdc_fact WHERE fact_id = ?",
                (_encode_key(before_id),),
            )
        self._store_fact(cur, merged, None, touched)

    def _apply_fact_delete(self, cur, row, touched):
        before = _present_values(row.get("values") or {}, row.get("none_sources"))
        fact_id = before.get(self._fact_id_column, _MISSING)
        if fact_id is _MISSING:
            raise CdcError("fact delete row carries no %s" % self._fact_id_column)
        cur.execute(
            "DELETE FROM cdc_fact WHERE fact_id = ?", (_encode_key(fact_id),)
        )

    # -- dimension rebuild / fact re-link -------------------------------------

    def _rebuild_dimension(self, cur, entity_text):
        cur.execute(
            "SELECT point_id, eff_time, attrs FROM cdc_raw_points"
            " WHERE entity_key = ?",
            (entity_text,),
        )
        points = []
        seen = {}
        for point_id_text, eff_text, attrs_text in cur.fetchall():
            if eff_text in seen:
                raise CdcDuplicatePointError(
                    entity_key=_decode_key(entity_text),
                    effective_time=_decode_key(eff_text),
                    point_ids=(
                        _decode_key(seen[eff_text]),
                        _decode_key(point_id_text),
                    ),
                )
            seen[eff_text] = point_id_text
            points.append((_sort_key(json.loads(eff_text)), eff_text, attrs_text))
        points.sort(key=lambda item: item[0])

        # Half-open intervals; consecutive identical attributes merge, the
        # raw points behind them stay in cdc_raw_points.
        intervals = []  # [valid_from, valid_to|None, attrs]
        for _, eff_text, attrs_text in points:
            if intervals and intervals[-1][2] == attrs_text:
                continue
            intervals.append([eff_text, None, attrs_text])
        for idx in range(len(intervals) - 1):
            intervals[idx][1] = intervals[idx + 1][0]

        cur.execute(
            "SELECT dim_id, valid_from, attrs FROM cdc_dim WHERE entity_key = ?",
            (entity_text,),
        )
        reusable = {}
        for dim_id, valid_from, attrs_text in cur.fetchall():
            reusable.setdefault((valid_from, attrs_text), dim_id)

        cur.execute("DELETE FROM cdc_dim WHERE entity_key = ?", (entity_text,))
        for valid_from, valid_to, attrs_text in intervals:
            # An interval whose starting point is unchanged keeps its dim_id.
            dim_id = reusable.pop((valid_from, attrs_text), None)
            if dim_id is None:
                dim_id = self._alloc_dim_id(cur)
            cur.execute(
                "INSERT INTO cdc_dim"
                " (dim_id, entity_key, valid_from, valid_to, attrs)"
                " VALUES (?, ?, ?, ?, ?)",
                (dim_id, entity_text, valid_from, valid_to, attrs_text),
            )

    def _relink_facts(self, cur, entity_text):
        cur.execute(
            "SELECT dim_id, valid_from, valid_to FROM cdc_dim"
            " WHERE entity_key = ?",
            (entity_text,),
        )
        intervals = []
        for dim_id, valid_from, valid_to in cur.fetchall():
            intervals.append(
                (
                    _sort_key(json.loads(valid_from)),
                    _sort_key(json.loads(valid_to))
                    if valid_to is not None
                    else None,
                    dim_id,
                )
            )
        intervals.sort(key=lambda item: item[0])

        cur.execute(
            "SELECT fact_id, event_time, dim_id FROM cdc_fact"
            " WHERE entity_key = ?",
            (entity_text,),
        )
        for fact_id_text, event_time_text, dim_id in cur.fetchall():
            new_dim_id = None
            encoded_time = json.loads(event_time_text)
            if encoded_time[0] != _T_NULL:
                key = _sort_key(encoded_time)
                for from_key, to_key, candidate in intervals:
                    if from_key <= key and (to_key is None or key < to_key):
                        new_dim_id = candidate
                        break
            if new_dim_id != dim_id:
                cur.execute(
                    "UPDATE cdc_fact SET dim_id = ? WHERE fact_id = ?",
                    (new_dim_id, fact_id_text),
                )

    # -- read helpers ---------------------------------------------------------

    def get_raw_points(self, entity_key=None):
        """Materialized raw points (decoded), ordered by entity and time."""
        with self._lock:
            cur = self._conn.cursor()
            if entity_key is None:
                cur.execute(
                    "SELECT point_id, entity_key, eff_time, attrs"
                    " FROM cdc_raw_points"
                )
            else:
                cur.execute(
                    "SELECT point_id, entity_key, eff_time, attrs"
                    " FROM cdc_raw_points WHERE entity_key = ?",
                    (_encode_key(entity_key),),
                )
            rows = [
                (
                    entity_text,
                    _sort_key(json.loads(eff_text)),
                    {
                        "point_id": _decode_key(point_id_text),
                        "entity_key": _decode_key(entity_text),
                        "effective_time": _decode_key(eff_text),
                        "attributes": _decode_attrs(attrs_text),
                    },
                )
                for point_id_text, entity_text, eff_text, attrs_text in cur.fetchall()
            ]
        rows.sort(key=lambda item: (item[0], item[1]))
        return [row for _, _, row in rows]

    def get_dimensions(self, entity_key=None):
        """Current SCD2 intervals (decoded), ordered by entity and start."""
        with self._lock:
            cur = self._conn.cursor()
            if entity_key is None:
                cur.execute(
                    "SELECT dim_id, entity_key, valid_from, valid_to, attrs"
                    " FROM cdc_dim"
                )
            else:
                cur.execute(
                    "SELECT dim_id, entity_key, valid_from, valid_to, attrs"
                    " FROM cdc_dim WHERE entity_key = ?",
                    (_encode_key(entity_key),),
                )
            rows = [
                (
                    entity_text,
                    _sort_key(json.loads(valid_from)),
                    {
                        "dim_id": dim_id,
                        "entity_key": _decode_key(entity_text),
                        "valid_from": _decode_key(valid_from),
                        "valid_to": _decode_key(valid_to)
                        if valid_to is not None
                        else None,
                        "attributes": _decode_attrs(attrs_text),
                    },
                )
                for dim_id, entity_text, valid_from, valid_to, attrs_text in (
                    cur.fetchall()
                )
            ]
        rows.sort(key=lambda item: (item[0], item[1]))
        return [row for _, _, row in rows]

    def get_facts(self, entity_key=None):
        """Facts (decoded) with their current dim_id reference."""
        with self._lock:
            cur = self._conn.cursor()
            if entity_key is None:
                cur.execute(
                    "SELECT fact_id, entity_key, event_time, dim_id, fields"
                    " FROM cdc_fact"
                )
            else:
                cur.execute(
                    "SELECT fact_id, entity_key, event_time, dim_id, fields"
                    " FROM cdc_fact WHERE entity_key = ?",
                    (_encode_key(entity_key),),
                )
            rows = [
                {
                    "fact_id": _decode_key(fact_id_text),
                    "entity_key": _decode_key(entity_text),
                    "event_time": _decode_key(event_time_text),
                    "dim_id": dim_id,
                    "fields": _decode_attrs(fields_text),
                }
                for fact_id_text, entity_text, event_time_text, dim_id, fields_text in (
                    cur.fetchall()
                )
            ]
        rows.sort(key=lambda row: _canonical_json(_encode_value(row["fact_id"])))
        return rows


# ---------------------------------------------------------------------------
# Transaction grouping helper
# ---------------------------------------------------------------------------


def _is_commit_query(event):
    query = getattr(event, "query", "")
    if isinstance(query, bytes):
        query = query.decode("utf-8", "replace")
    return query.strip().upper() == "COMMIT"


def group_transactions(stream):
    """Group a ``BinLogStreamReader`` event stream into native transactions.

    Yields ``(events, log_file, log_pos)`` tuples where ``events`` are the
    row events of one transaction and ``log_file`` / ``log_pos`` are the
    binlog coordinates at its end (the XidEvent or COMMIT QueryEvent), ready
    to be passed to :meth:`CdcDimensionBackfill.apply_transaction`::

        for events, log_file, log_pos in group_transactions(stream):
            backfill.apply_transaction(events, log_file, log_pos)

    Transactions without row events still yield (with an empty event list)
    so the checkpoint advances past them.  A trailing run of row events
    without a terminating XidEvent/COMMIT is an incomplete transaction and
    is not yielded.
    """
    pending = []
    for event in stream:
        if _resolve_row_kind(event) is not None:
            pending.append(event)
            continue
        names = _class_names(event)
        if "XidEvent" in names or ("QueryEvent" in names and _is_commit_query(event)):
            yield pending, getattr(stream, "log_file", None), getattr(
                stream, "log_pos", None
            )
            pending = []
