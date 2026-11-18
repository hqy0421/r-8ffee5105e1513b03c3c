"""Tests for pymysqlreplication.cdc (CDC dimension backfill).

These tests are self-contained: they feed duck-typed row events (same class
names and row payload shape as the real ones) straight into
CdcDimensionBackfill and assert on the SQLite state, no MySQL server needed.
"""

import datetime
import decimal

import pytest

from pymysqlreplication import BinLogStreamReader  # noqa: F401  (old API intact)
from pymysqlreplication.cdc import (
    CdcDimensionBackfill,
    CdcDuplicatePointError,
    CdcError,
    group_transactions,
)
from pymysqlreplication.constants import NONE_SOURCE

T1 = datetime.datetime(2026, 1, 1, 10, 0, 0)
T2 = datetime.datetime(2026, 1, 2, 10, 0, 0)
T3 = datetime.datetime(2026, 1, 3, 10, 0, 0)
T0 = datetime.datetime(2025, 12, 31, 10, 0, 0)


# ---------------------------------------------------------------------------
# Fake binlog events (duck-typed: class names drive kind resolution)
# ---------------------------------------------------------------------------


class WriteRowsEvent:
    def __init__(self, table, rows, schema="cdc"):
        self.schema = schema
        self.table = table
        self.rows = rows


class UpdateRowsEvent:
    def __init__(self, table, rows, schema="cdc"):
        self.schema = schema
        self.table = table
        self.rows = rows


class DeleteRowsEvent:
    def __init__(self, table, rows, schema="cdc"):
        self.schema = schema
        self.table = table
        self.rows = rows


class PartialUpdateRowsEvent(UpdateRowsEvent):
    pass


class XidEvent:
    pass


class QueryEvent:
    def __init__(self, query):
        self.query = query


def write(table, values, none_sources=None, schema="cdc"):
    return WriteRowsEvent(
        table, [{"values": values, "none_sources": none_sources or {}}], schema
    )


def update(table, before, after, before_ns=None, after_ns=None, schema="cdc"):
    return UpdateRowsEvent(
        table,
        [
            {
                "before_values": before,
                "before_none_sources": before_ns or {},
                "after_values": after,
                "after_none_sources": after_ns or {},
            }
        ],
        schema,
    )


def delete(table, values, none_sources=None, schema="cdc"):
    return DeleteRowsEvent(
        table, [{"values": values, "none_sources": none_sources or {}}], schema
    )


def make_cdc(**kwargs):
    return CdcDimensionBackfill(":memory:", **kwargs)


def point(point_id, entity, eff, **attrs):
    values = {"pointID": point_id, "entityKey": entity, "effective_time": eff}
    values.update(attrs)
    return values


def fact(fact_id, entity, event_time, **fields):
    values = {"factID": fact_id, "entityKey": entity, "event_time": event_time}
    values.update(fields)
    return values


# ---------------------------------------------------------------------------
# Dimension building + fact linking
# ---------------------------------------------------------------------------


def test_basic_dimension_intervals_and_half_open_fact_linking():
    cdc = make_cdc()
    events = [
        write("point", point(1, "e1", T1, color="red")),
        write("point", point(2, "e1", T2, color="blue")),
        write("fact", fact(1, "e1", T0)),  # before first interval -> NULL
        write("fact", fact(2, "e1", T1)),  # exactly at start -> first interval
        write("fact", fact(3, "e1", T1 + datetime.timedelta(hours=1))),
        write("fact", fact(4, "e1", T2)),  # half-open: end == next start
        write("fact", fact(5, "e1", T3)),
    ]
    assert cdc.apply_transaction(events, "mysql-bin.000001", 100) is True

    dims = cdc.get_dimensions("e1")
    assert [(d["valid_from"], d["valid_to"]) for d in dims] == [(T1, T2), (T2, None)]
    assert [d["attributes"] for d in dims] == [{"color": "red"}, {"color": "blue"}]

    facts = {f["fact_id"]: f["dim_id"] for f in cdc.get_facts("e1")}
    first, second = dims[0]["dim_id"], dims[1]["dim_id"]
    assert facts == {1: None, 2: first, 3: first, 4: second, 5: second}
    assert cdc.checkpoint == ("mysql-bin.000001", 100)


def test_consecutive_same_attributes_merged_but_raw_points_kept():
    cdc = make_cdc()
    events = [
        write("point", point(1, "e1", T1, x=1)),
        write("point", point(2, "e1", T2, x=1)),  # same attrs -> merged
        write("point", point(3, "e1", T3, x=2)),
    ]
    assert cdc.apply_transaction(events, "f.000001", 10) is True

    dims = cdc.get_dimensions("e1")
    assert [(d["valid_from"], d["valid_to"], d["attributes"]) for d in dims] == [
        (T1, T3, {"x": 1}),
        (T3, None, {"x": 2}),
    ]
    # same-value raw points are all preserved
    raw = cdc.get_raw_points("e1")
    assert [(r["point_id"], r["effective_time"]) for r in raw] == [
        (1, T1),
        (2, T2),
        (3, T3),
    ]


def test_minimal_image_distinguishes_missing_column_from_null():
    cdc = make_cdc()
    # FULL insert image
    assert cdc.apply_transaction(
        [write("point", point(1, "e1", T1, a=1, b="x", c="y"))], "f.000001", 10
    )
    # MINIMAL update image: only pointID + changed column b (set to NULL).
    # a and c are missing from the image (cols bitmap) and must be backfilled.
    after_ns = {
        "a": NONE_SOURCE.COLS_BITMAP,
        "c": NONE_SOURCE.COLS_BITMAP,
        "b": NONE_SOURCE.NULL,
    }
    before_ns = {col: NONE_SOURCE.COLS_BITMAP for col in ("a", "b", "c")}
    assert cdc.apply_transaction(
        [
            update(
                "point",
                {"pointID": 1},
                {"pointID": 1, "b": None},
                before_ns=before_ns,
                after_ns=after_ns,
            )
        ],
        "f.000001",
        20,
    )
    (raw,) = cdc.get_raw_points("e1")
    assert raw["attributes"] == {"a": 1, "b": None, "c": "y"}

    # A column never present in any image stays absent (not NULL).
    assert cdc.apply_transaction(
        [
            update(
                "point",
                {"pointID": 1},
                {"pointID": 1, "a": 2},
                before_ns=before_ns,
                after_ns={
                    "b": NONE_SOURCE.COLS_BITMAP,
                    "c": NONE_SOURCE.COLS_BITMAP,
                },
            )
        ],
        "f.000001",
        30,
    )
    (raw,) = cdc.get_raw_points("e1")
    assert raw["attributes"] == {"a": 2, "b": None, "c": "y"}
    dims = cdc.get_dimensions("e1")
    assert dims[0]["attributes"] == {"a": 2, "b": None, "c": "y"}


def test_explicit_null_vs_missing_in_insert_image():
    cdc = make_cdc()
    none_sources = {"b": NONE_SOURCE.NULL, "c": NONE_SOURCE.COLS_BITMAP}
    assert cdc.apply_transaction(
        [
            write(
                "point",
                {"pointID": 1, "entityKey": "e1", "effective_time": T1, "b": None},
                none_sources=none_sources,
            )
        ],
        "f.000001",
        10,
    )
    (raw,) = cdc.get_raw_points("e1")
    # b is an explicit NULL (present), c is missing from the image (absent)
    assert raw["attributes"] == {"b": None}


def test_empty_set_and_json_partial_update_none_sources():
    cdc = make_cdc()
    # EMPTY_SET decodes to None but means a real empty set; JSON partial
    # update means "same with before values" and must be backfilled.
    none_sources = {
        "tags": NONE_SOURCE.EMPTY_SET,
        "doc": NONE_SOURCE.JSON_PARTIAL_UPDATE,
    }
    assert cdc.apply_transaction(
        [
            write(
                "point",
                {
                    "pointID": 1,
                    "entityKey": "e1",
                    "effective_time": T1,
                    "tags": None,
                    "doc": None,
                    "v": "A",
                },
                none_sources=none_sources,
            )
        ],
        "f.000001",
        10,
    )
    (raw,) = cdc.get_raw_points("e1")
    # empty set restored; doc was never seen -> stays absent
    assert raw["attributes"] == {"tags": set(), "v": "A"}

    # a later FULL image provides the JSON doc; a partial update afterwards
    # must keep the stored doc instead of nulling it
    assert cdc.apply_transaction(
        [write("point", point(2, "e2", T1, doc={"a": 1}))], "f.000001", 20
    )
    assert cdc.apply_transaction(
        [
            update(
                "point",
                {"pointID": 2},
                {"pointID": 2, "doc": None},
                after_ns={"doc": NONE_SOURCE.JSON_PARTIAL_UPDATE},
            )
        ],
        "f.000001",
        30,
    )
    (raw,) = cdc.get_raw_points("e2")
    assert raw["attributes"] == {"doc": {"a": 1}}


def test_deleting_merged_same_value_point_keeps_dim_ids():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [
            write("point", point(1, "e1", T1, v="A")),
            write("point", point(2, "e1", T2, v="A")),  # merges into [T1, T3)
            write("point", point(3, "e1", T3, v="B")),
            write("fact", fact(1, "e1", T2)),
        ],
        "f.000001",
        10,
    )
    dims_before = cdc.get_dimensions("e1")
    facts_before = cdc.get_facts("e1")
    assert len(dims_before) == 2

    # deleting the merged-away raw point changes nothing in the dimension
    assert cdc.apply_transaction(
        [delete("point", {"pointID": 2})], "f.000001", 20
    )
    assert cdc.get_dimensions("e1") == dims_before
    assert cdc.get_facts("e1") == facts_before
    # ...but the raw point is really gone
    assert [r["point_id"] for r in cdc.get_raw_points("e1")] == [1, 3]


def test_fact_and_point_order_independent_within_transaction():
    cdc = make_cdc()
    # fact arrives before its dimension point inside the same transaction
    assert cdc.apply_transaction(
        [
            write("fact", fact(1, "e1", T1)),
            write("point", point(1, "e1", T1, v="A")),
        ],
        "f.000001",
        10,
    )
    (dim,) = cdc.get_dimensions("e1")
    assert cdc.get_facts("e1")[0]["dim_id"] == dim["dim_id"]


# ---------------------------------------------------------------------------
# Late arrival / correction / deletion / time migration
# ---------------------------------------------------------------------------


def test_late_arrival_rebuilds_and_keeps_dim_id_of_unchanged_starts():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [
            write("point", point(1, "e1", T1, v="A")),
            write("point", point(3, "e1", T3, v="B")),
            write("fact", fact(1, "e1", T2)),
        ],
        "f.000001",
        10,
    )
    dims = cdc.get_dimensions("e1")
    id_a, id_b = dims[0]["dim_id"], dims[1]["dim_id"]
    assert cdc.get_facts("e1")[0]["dim_id"] == id_a

    # late point arriving between T1 and T3
    assert cdc.apply_transaction(
        [write("point", point(2, "e1", T2, v="C"))], "f.000001", 20
    )
    dims = cdc.get_dimensions("e1")
    assert [(d["valid_from"], d["valid_to"], d["attributes"]) for d in dims] == [
        (T1, T2, {"v": "A"}),
        (T2, T3, {"v": "C"}),
        (T3, None, {"v": "B"}),
    ]
    # intervals whose starting point is unchanged keep their dim_id
    assert dims[0]["dim_id"] == id_a
    assert dims[2]["dim_id"] == id_b
    assert dims[1]["dim_id"] not in (id_a, id_b)
    # the fact is re-hung onto the interval now containing its event time
    assert cdc.get_facts("e1")[0]["dim_id"] == dims[1]["dim_id"]


def test_correction_changes_attributes_and_relinks_facts():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [
            write("point", point(1, "e1", T1, v="A")),
            write("point", point(2, "e1", T2, v="B")),
            write("fact", fact(1, "e1", T3)),
        ],
        "f.000001",
        10,
    )
    dims = cdc.get_dimensions("e1")
    id_a, id_b = dims[0]["dim_id"], dims[1]["dim_id"]
    assert cdc.get_facts("e1")[0]["dim_id"] == id_b

    # correction: same point, same time, new attributes (FULL after image)
    assert cdc.apply_transaction(
        [
            update(
                "point",
                point(2, "e1", T2, v="B"),
                point(2, "e1", T2, v="B2"),
            )
        ],
        "f.000001",
        20,
    )
    dims = cdc.get_dimensions("e1")
    assert [(d["valid_from"], d["valid_to"], d["attributes"]) for d in dims] == [
        (T1, T2, {"v": "A"}),
        (T2, None, {"v": "B2"}),
    ]
    assert dims[0]["dim_id"] == id_a  # unchanged start keeps dim_id
    assert dims[1]["dim_id"] != id_b  # corrected point is a new version
    assert cdc.get_facts("e1")[0]["dim_id"] == dims[1]["dim_id"]


def test_deletion_rebuilds_intervals():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [
            write("point", point(1, "e1", T1, v="A")),
            write("point", point(2, "e1", T2, v="B")),
            write("point", point(3, "e1", T3, v="C")),
            write("fact", fact(1, "e1", T2)),
        ],
        "f.000001",
        10,
    )
    dims = cdc.get_dimensions("e1")
    id_a, _, id_c = (d["dim_id"] for d in dims)

    assert cdc.apply_transaction(
        [delete("point", {"pointID": 2}, none_sources={})], "f.000001", 20
    )
    dims = cdc.get_dimensions("e1")
    assert [(d["valid_from"], d["valid_to"], d["attributes"]) for d in dims] == [
        (T1, T3, {"v": "A"}),
        (T3, None, {"v": "C"}),
    ]
    assert dims[0]["dim_id"] == id_a
    assert dims[1]["dim_id"] == id_c
    # fact at T2 now falls into the rebuilt first interval
    assert cdc.get_facts("e1")[0]["dim_id"] == id_a
    assert [r["point_id"] for r in cdc.get_raw_points("e1")] == [1, 3]


def test_time_migration_rebuilds():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [
            write("point", point(1, "e1", T1, v="A")),
            write("point", point(2, "e1", T2, v="B")),
            write("fact", fact(1, "e1", T0 + datetime.timedelta(hours=12))),
        ],
        "f.000001",
        10,
    )
    assert cdc.get_facts("e1")[0]["dim_id"] is None  # T0.5 < T1

    # point 2 migrates from T2 to T0 (before T1)
    assert cdc.apply_transaction(
        [
            update(
                "point",
                {"pointID": 2, "effective_time": T2},
                {"pointID": 2, "effective_time": T0},
                after_ns={},
            )
        ],
        "f.000001",
        20,
    )
    dims = cdc.get_dimensions("e1")
    assert [(d["valid_from"], d["valid_to"], d["attributes"]) for d in dims] == [
        (T0, T1, {"v": "B"}),
        (T1, None, {"v": "A"}),
    ]
    # the fact at T0.5 now matches the migrated interval
    assert cdc.get_facts("e1")[0]["dim_id"] == dims[0]["dim_id"]


def test_entity_key_change_moves_point_between_entities():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [
            write("point", point(1, "e1", T1, v="A")),
            write("point", point(2, "e2", T2, v="X")),
        ],
        "f.000001",
        10,
    )
    assert cdc.apply_transaction(
        [
            update(
                "point",
                point(1, "e1", T1, v="A"),
                point(1, "e2", T1, v="A"),
            )
        ],
        "f.000001",
        20,
    )
    assert cdc.get_dimensions("e1") == []
    dims_e2 = cdc.get_dimensions("e2")
    assert [(d["valid_from"], d["valid_to"], d["attributes"]) for d in dims_e2] == [
        (T1, T2, {"v": "A"}),
        (T2, None, {"v": "X"}),
    ]


def test_point_primary_key_change():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [write("point", point(1, "e1", T1, v="A"))], "f.000001", 10
    )
    assert cdc.apply_transaction(
        [
            update(
                "point",
                {"pointID": 1, "entityKey": "e1", "effective_time": T1, "v": "A"},
                {"pointID": 2, "entityKey": "e1", "effective_time": T1, "v": "A"},
            )
        ],
        "f.000001",
        20,
    )
    raw = cdc.get_raw_points("e1")
    assert [r["point_id"] for r in raw] == [2]
    assert raw[0]["attributes"] == {"v": "A"}
    assert len(cdc.get_dimensions("e1")) == 1


# ---------------------------------------------------------------------------
# Duplicate rejection + atomicity
# ---------------------------------------------------------------------------


def test_duplicate_point_same_key_and_time_rejected_state_untouched():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [write("point", point(1, "e1", T1, v="A"))], "f.000001", 10
    )
    before_dims = cdc.get_dimensions("e1")
    before_raw = cdc.get_raw_points("e1")

    with pytest.raises(CdcDuplicatePointError) as excinfo:
        cdc.apply_transaction(
            [write("point", point(2, "e1", T1, v="B"))], "f.000001", 20
        )
    assert excinfo.value.entity_key == "e1"
    assert excinfo.value.effective_time == T1
    assert set(excinfo.value.point_ids) == {1, 2}

    # failure keeps everything old: dims, raw points and checkpoint
    assert cdc.get_dimensions("e1") == before_dims
    assert cdc.get_raw_points("e1") == before_raw
    assert cdc.checkpoint == ("f.000001", 10)


def test_failed_transaction_rolls_back_all_tables():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [
            write("point", point(1, "e1", T1, v="A")),
            write("fact", fact(1, "e1", T1)),
        ],
        "f.000001",
        10,
    )
    before = (
        cdc.get_dimensions(),
        cdc.get_raw_points(),
        cdc.get_facts(),
        cdc.checkpoint,
    )
    # one valid insert + one duplicate in the same transaction -> all rolled back
    with pytest.raises(CdcDuplicatePointError):
        cdc.apply_transaction(
            [
                write("point", point(2, "e1", T2, v="B")),
                write("fact", fact(2, "e1", T2)),
                write("point", point(3, "e1", T2, v="C")),
            ],
            "f.000001",
            20,
        )
    after = (
        cdc.get_dimensions(),
        cdc.get_raw_points(),
        cdc.get_facts(),
        cdc.checkpoint,
    )
    assert before == after


# ---------------------------------------------------------------------------
# Idempotent replay / reopen and continue
# ---------------------------------------------------------------------------


def test_complete_transaction_replay_is_idempotent():
    cdc = make_cdc()
    events = [
        write("point", point(1, "e1", T1, v="A")),
        write("fact", fact(1, "e1", T1)),
    ]
    assert cdc.apply_transaction(events, "f.000001", 100) is True
    state = (cdc.get_dimensions(), cdc.get_raw_points(), cdc.get_facts())

    # exact replay -> skipped
    assert cdc.apply_transaction(events, "f.000001", 100) is False
    # older position -> skipped
    assert cdc.apply_transaction(events, "f.000001", 50) is False
    # older file -> skipped
    assert cdc.apply_transaction(events, "f.000000", 99999) is False
    assert (cdc.get_dimensions(), cdc.get_raw_points(), cdc.get_facts()) == state
    assert cdc.checkpoint == ("f.000001", 100)

    # next transaction applies, then its own replay is skipped
    assert cdc.apply_transaction(
        [write("point", point(2, "e1", T2, v="B"))], "f.000001", 200
    ) is True
    assert cdc.apply_transaction(
        [write("point", point(2, "e1", T2, v="B"))], "f.000001", 200
    ) is False
    assert len(cdc.get_raw_points("e1")) == 2
    # rotation to the next file is ordered correctly
    assert cdc.apply_transaction([], "f.000002", 4) is True
    assert cdc.checkpoint == ("f.000002", 4)


def test_reopen_continues_from_checkpoint(tmp_path):
    db = str(tmp_path / "state.sqlite")
    cdc = CdcDimensionBackfill(db)
    assert cdc.apply_transaction(
        [write("point", point(1, "e1", T1, v="A"))], "f.000001", 100
    )
    assert cdc.checkpoint == ("f.000001", 100)
    cdc.close()

    reopened = CdcDimensionBackfill(db)
    assert reopened.checkpoint == ("f.000001", 100)
    # replay of the last transaction is skipped on the reopened database
    assert reopened.apply_transaction(
        [write("point", point(1, "e1", T1, v="A"))], "f.000001", 100
    ) is False
    assert reopened.apply_transaction(
        [write("point", point(2, "e1", T2, v="B"))], "f.000001", 200
    ) is True
    assert len(reopened.get_dimensions("e1")) == 2
    reopened.close()


def test_unrelated_tables_ignored_but_checkpoint_advances():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [write("other_table", {"id": 1}), write("point", point(1, "e1", T1))],
        "f.000001",
        10,
    )
    assert len(cdc.get_raw_points()) == 1
    assert cdc.checkpoint == ("f.000001", 10)


def test_schema_filter():
    cdc = make_cdc(schema="cdc")
    assert cdc.apply_transaction(
        [
            write("point", point(1, "e1", T1), schema="other_schema"),
            write("point", point(2, "e1", T2), schema="cdc"),
        ],
        "f.000001",
        10,
    )
    assert [r["point_id"] for r in cdc.get_raw_points()] == [2]


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------


def test_fact_update_relinks_and_keeps_other_fields():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [
            write("point", point(1, "e1", T1, v="A")),
            write("point", point(2, "e1", T2, v="B")),
            write("fact", fact(1, "e1", T1, amount=10, note="n")),
        ],
        "f.000001",
        10,
    )
    dims = cdc.get_dimensions("e1")
    id_a, id_b = dims[0]["dim_id"], dims[1]["dim_id"]
    assert cdc.get_facts("e1")[0]["dim_id"] == id_a

    # MINIMAL image: only the event time changes
    minimal_ns = {
        "entityKey": NONE_SOURCE.COLS_BITMAP,
        "amount": NONE_SOURCE.COLS_BITMAP,
        "note": NONE_SOURCE.COLS_BITMAP,
    }
    assert cdc.apply_transaction(
        [
            update(
                "fact",
                {"factID": 1},
                {"factID": 1, "event_time": T2},
                before_ns=minimal_ns,
                after_ns=minimal_ns,
            )
        ],
        "f.000001",
        20,
    )
    (f,) = cdc.get_facts("e1")
    assert f["dim_id"] == id_b
    assert f["event_time"] == T2
    assert f["fields"] == {"amount": 10, "note": "n"}  # other fields unchanged

    # updating a non-time field does not move the reference
    assert cdc.apply_transaction(
        [
            update(
                "fact",
                fact(1, "e1", T2, amount=10, note="n"),
                fact(1, "e1", T2, amount=99, note="n"),
            )
        ],
        "f.000001",
        30,
    )
    (f,) = cdc.get_facts("e1")
    assert f["dim_id"] == id_b
    assert f["fields"] == {"amount": 99, "note": "n"}


def test_fact_delete():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [write("fact", fact(1, "e1", T1, amount=1))], "f.000001", 10
    )
    assert len(cdc.get_facts()) == 1
    assert cdc.apply_transaction(
        [delete("fact", {"factID": 1})], "f.000001", 20
    )
    assert cdc.get_facts() == []


def test_fact_without_matching_interval_gets_null():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [write("fact", fact(1, "ghost", T1, amount=1))], "f.000001", 10
    )
    (f,) = cdc.get_facts()
    assert f["dim_id"] is None
    assert f["fields"] == {"amount": 1}


# ---------------------------------------------------------------------------
# Precision
# ---------------------------------------------------------------------------


def test_declared_time_and_attributes_keep_precision():
    cdc = make_cdc()
    precise = datetime.datetime(2026, 1, 1, 10, 0, 0, 123456)
    attrs = {
        "price": decimal.Decimal("19.99"),
        "qty": 123456789012345678901234567890,
        "ratio": 0.1,
        "name": "café ☕",
        "blob": b"\x00\x01\xff",
        "flag": True,
        "tags": {"b", "a"},
        "when": datetime.datetime(2026, 6, 30, 23, 59, 59, 999999),
        "day": datetime.date(2026, 2, 28),
        "nothing": None,
    }
    assert cdc.apply_transaction(
        [write("point", point(1, "e1", precise, **attrs))], "f.000001", 10
    )
    (raw,) = cdc.get_raw_points("e1")
    assert raw["effective_time"] == precise
    assert raw["attributes"] == attrs
    assert isinstance(raw["attributes"]["price"], decimal.Decimal)
    assert isinstance(raw["attributes"]["qty"], int)
    (dim,) = cdc.get_dimensions("e1")
    assert dim["valid_from"] == precise
    assert dim["attributes"] == attrs


def test_int_and_string_entity_keys_are_distinct():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [
            write("point", point(1, 5, T1, v="int")),
            write("point", point(2, "5", T1, v="str")),
        ],
        "f.000001",
        10,
    )
    assert [d["attributes"] for d in cdc.get_dimensions(5)] == [{"v": "int"}]
    assert [d["attributes"] for d in cdc.get_dimensions("5")] == [{"v": "str"}]


# ---------------------------------------------------------------------------
# Transaction grouping
# ---------------------------------------------------------------------------


class FakeStream:
    def __init__(self, steps):
        self._steps = steps
        self.log_file = None
        self.log_pos = None

    def __iter__(self):
        for event, log_file, log_pos in self._steps:
            self.log_file, self.log_pos = log_file, log_pos
            yield event


def test_group_transactions():
    w1 = write("point", point(1, "e1", T1))
    w2 = write("point", point(2, "e1", T2))
    w3 = write("point", point(3, "e1", T3))
    stream = FakeStream(
        [
            (QueryEvent("BEGIN"), "f.000001", 90),
            (w1, "f.000001", 100),
            (XidEvent(), "f.000001", 120),
            (w2, "f.000001", 200),
            (QueryEvent("COMMIT"), "f.000001", 220),
            (w3, "f.000001", 300),  # never committed -> not yielded
        ]
    )
    txs = list(group_transactions(stream))
    assert len(txs) == 2
    assert txs[0] == ([w1], "f.000001", 120)
    assert txs[1] == ([w2], "f.000001", 220)

    # the grouped transactions feed the backfill directly
    cdc = make_cdc()
    for events, log_file, log_pos in txs:
        assert cdc.apply_transaction(events, log_file, log_pos) is True
    assert [r["point_id"] for r in cdc.get_raw_points("e1")] == [1, 2]
    assert cdc.checkpoint == ("f.000001", 220)


def test_group_transactions_yields_empty_transactions_for_checkpoint():
    stream = FakeStream([(XidEvent(), "f.000001", 50)])
    assert list(group_transactions(stream)) == [([], "f.000001", 50)]


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------


def test_partial_update_rows_event_treated_as_update():
    cdc = make_cdc()
    assert cdc.apply_transaction(
        [write("point", point(1, "e1", T1, v="A"))], "f.000001", 10
    )
    event = PartialUpdateRowsEvent(
        "point",
        [
            {
                "before_values": {"pointID": 1},
                "before_none_sources": {},
                "after_values": {"pointID": 1, "v": "A2"},
                "after_none_sources": {},
            }
        ],
    )
    assert cdc.apply_transaction([event], "f.000001", 20)
    assert cdc.get_dimensions("e1")[0]["attributes"] == {"v": "A2"}


def test_update_without_stored_point_uses_before_image():
    cdc = make_cdc()
    # mid-stream start: no stored point, FULL before/after images
    assert cdc.apply_transaction(
        [update("point", point(1, "e1", T1, v="A"), point(1, "e1", T1, v="B"))],
        "f.000001",
        10,
    )
    (raw,) = cdc.get_raw_points("e1")
    assert raw["attributes"] == {"v": "B"}


def test_delete_unknown_point_is_noop():
    cdc = make_cdc()
    assert cdc.apply_transaction([delete("point", {"pointID": 42})], "f.000001", 10)
    assert cdc.get_raw_points() == []
    assert cdc.checkpoint == ("f.000001", 10)


def test_missing_point_id_raises_and_rolls_back():
    cdc = make_cdc()
    with pytest.raises(CdcError):
        cdc.apply_transaction(
            [write("point", {"entityKey": "e1", "effective_time": T1})],
            "f.000001",
            10,
        )
    assert cdc.checkpoint is None
    assert cdc.get_raw_points() == []


def test_context_manager(tmp_path):
    db = str(tmp_path / "ctx.sqlite")
    with CdcDimensionBackfill(db) as cdc:
        assert cdc.apply_transaction(
            [write("point", point(1, "e1", T1))], "f.000001", 10
        )
    with CdcDimensionBackfill(db) as cdc:
        assert cdc.checkpoint == ("f.000001", 10)


def test_old_api_unchanged():
    import pymysqlreplication

    assert pymysqlreplication.BinLogStreamReader is BinLogStreamReader
    for name in (
        "CdcDimensionBackfill",
        "CdcDuplicatePointError",
        "CdcError",
        "group_transactions",
    ):
        assert hasattr(pymysqlreplication, name)
