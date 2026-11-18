CDC dimension backfill
======================

The :mod:`pymysqlreplication.cdc` module turns grouped native transactions
into a SQLite-backed slowly-changing-dimension (SCD2, half-open interval)
model. It consumes the row events of one transaction plus the binlog
file/pos at the end of that transaction, and maintains:

* ``cdc_raw_points`` - every materialized dimension change point
  (``pointID`` keyed). Same-value points are all preserved, even when the
  intervals they produce get merged.
* ``cdc_dim`` - half-open intervals ``[valid_from, valid_to)`` keyed by
  ``entityKey`` / effective time, with a stable surrogate ``dim_id``.
* ``cdc_fact`` - facts (``factID`` keyed) whose ``dim_id`` reference is
  re-linked by event time; every other field is left untouched.
* ``cdc_meta`` - the binlog checkpoint (file/pos) and bookkeeping counters.

Highlights:

* ``before_values`` / ``after_values`` and ``none_sources`` are used to tell
  a column *missing* from the binlog image (``binlog_row_image=MINIMAL``)
  apart from an explicit ``NULL``; missing columns are backfilled from the
  materialized state.
* Consecutive intervals with identical attributes are merged.
* Late arrivals, corrections, deletions and effective-time migrations
  rebuild the affected ``entityKey`` intervals; two points sharing the same
  ``entityKey`` and effective time are rejected.
* An interval whose starting point is unchanged keeps its ``dim_id``.
* Raw points/facts, the dimension, the facts and the checkpoint commit in
  one SQLite transaction: failures keep the old state, replaying a complete
  transaction is idempotent, and a reopened database continues from the
  checkpoint.
* Declared times and attributes are stored in a typed canonical encoding
  (``Decimal``, ``datetime`` with microseconds, big ints, bytes, ... keep
  their exact value).

Example
-------

.. code-block:: python

    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.cdc import CdcDimensionBackfill, group_transactions

    stream = BinLogStreamReader(connection_settings, server_id=1024,
                                only_tables=["point", "fact"])
    backfill = CdcDimensionBackfill("cdc_state.sqlite")
    try:
        for events, log_file, log_pos in group_transactions(stream):
            backfill.apply_transaction(events, log_file, log_pos)
    finally:
        stream.close()
        backfill.close()

API
---

.. autoclass:: pymysqlreplication.cdc.CdcDimensionBackfill
    :members:

.. autoclass:: pymysqlreplication.cdc.CdcDuplicatePointError

.. autofunction:: pymysqlreplication.cdc.group_transactions
