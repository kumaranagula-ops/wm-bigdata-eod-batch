"""Unit tests for the business logic, on tiny hand-built DataFrames with edge cases."""
import datetime as dt
from decimal import Decimal as D

from pyspark.sql import functions as F

from wm_pipeline.transforms.lots import fifo_matches, lot_ledger, open_lots, oversold
from wm_pipeline.transforms.scd import row_hash, stage_scd2
from wm_pipeline.transforms.trades import latest_trade_versions, split_adjust, validate_trades

TRADE_SCHEMA = ("trade_id STRING, account_id BIGINT, security_id STRING, trade_date DATE, "
                "exec_ts TIMESTAMP, side STRING, quantity DECIMAL(18,4), price DECIMAL(18,6), "
                "commission DECIMAL(18,2)")


def trade(tid, d, side, qty, price, comm=0, acct=1, sec="S1"):
    return (tid, acct, sec, dt.date.fromisoformat(d), dt.datetime.fromisoformat(f"{d} 15:00:00"),
            side, D(str(qty)), D(str(price)), D(str(comm)))


def test_fifo_relieves_oldest_lots_first(spark):
    trades = spark.createDataFrame([
        trade("B1", "2024-01-02", "BUY", 100, 10),
        trade("B2", "2025-06-02", "BUY", 50, 20),
        trade("B3", "2026-03-02", "BUY", 150, 30),
        trade("S1", "2026-05-01", "SELL", 120, 40),
        trade("S2", "2026-06-01", "SELL", 40, 50),
    ], TRADE_SCHEMA)
    buys, sells = lot_ledger(trades)

    m = {(r.sell_trade_id, r.lot_id): r for r in fifo_matches(buys, sells).collect()}
    assert {k: float(v.quantity) for k, v in m.items()} == {
        ("S1", "B1"): 100, ("S1", "B2"): 20, ("S2", "B2"): 30, ("S2", "B3"): 10}
    # B1 held > 1 year -> long term; B2 sold < 1 year after purchase -> short term
    assert m[("S1", "B1")].term == "LONG_TERM"
    assert m[("S1", "B2")].term == "SHORT_TERM"
    assert m[("S1", "B1")].realized_gl == D("3000.00")   # 100 x (40 - 10)

    lots = {r.lot_id: float(r.remaining_qty) for r in open_lots(buys, sells, "2026-06-30").collect()}
    assert lots == {"B3": 140.0}
    assert oversold(buys, sells).count() == 0


def test_commission_is_in_cost_and_proceeds(spark):
    trades = spark.createDataFrame([
        trade("B1", "2026-01-02", "BUY", 10, 100, comm=10),
        trade("S1", "2026-02-02", "SELL", 10, 110, comm=10),
    ], TRADE_SCHEMA)
    buys, sells = lot_ledger(trades)
    r = fifo_matches(buys, sells).first()
    assert r.cost_basis == D("1010.00") and r.proceeds == D("1090.00") and r.realized_gl == D("80.00")


def test_oversold_is_detected(spark):
    trades = spark.createDataFrame([trade("B1", "2026-01-02", "BUY", 10, 1),
                                    trade("S1", "2026-01-03", "SELL", 15, 1)], TRADE_SCHEMA)
    buys, sells = lot_ledger(trades)
    assert oversold(buys, sells).count() == 1


def test_split_adjust_restates_pre_split_trades_only(spark):
    trades = spark.createDataFrame([
        trade("B1", "2026-11-02", "BUY", 100, 50),
        trade("B2", "2026-11-10", "BUY", 10, 25),   # on the ex-date: already post-split
    ], TRADE_SCHEMA)
    splits = spark.createDataFrame([("S1", dt.date(2026, 11, 10), 2.0)], "security_id STRING, ex_date DATE, ratio DOUBLE")
    out = {r.trade_id: r for r in split_adjust(trades, splits).collect()}
    assert out["B1"].quantity == D("200.0000") and out["B1"].price == D("25.000000")
    assert out["B2"].quantity == D("10.0000") and out["B2"].price == D("25.000000")


def test_latest_version_wins_and_file_beats_stream(spark):
    rows = [("T1", 1, "STREAM", "NEW"), ("T1", 1, "FILE", "NEW"), ("T2", 1, "FILE", "NEW"),
            ("T2", 2, "FILE", "CANCEL")]
    df = spark.createDataFrame(rows, "trade_id STRING, version INT, record_source STRING, trade_status STRING") \
              .withColumn("exec_ts", F.current_timestamp())
    out = {r.trade_id: (r.record_source, r.trade_status) for r in latest_trade_versions(df).collect()}
    assert out == {"T1": ("FILE", "NEW"), "T2": ("FILE", "CANCEL")}


def test_validate_trades_reasons(spark):
    schema = TRADE_SCHEMA + ", settle_date DATE, _corrupt_record STRING"
    d = dt.date(2026, 11, 9)
    good = trade("T1", "2026-11-09", "BUY", 1, 10) + (d, None)
    rows = [good,
            trade("T2", "2026-11-09", "BUY", 1, 10, acct=999) + (d, None),        # orphan
            trade("T3", "2026-11-09", "HOLD", 1, 10) + (d, None),                 # bad side
            ("T4", None, "S1", d, None, "BUY", D(1), D(1), D(0), d, None),        # no account
            ("T5", 1, None, None, None, None, None, None, None, None, "T5,1,abc"),  # malformed
            trade("T6", "2026-11-09", "BUY", 1, 10, sec="NOPE") + (d, None)]      # unknown security
    trades = spark.createDataFrame(rows, schema)
    accounts = spark.createDataFrame([(1,)], "account_id BIGINT")
    securities = spark.createDataFrame([("S1",)], "security_id STRING")
    valid, rejected = validate_trades(trades, accounts, securities)
    assert [r.trade_id for r in valid.collect()] == ["T1"]
    assert {r.trade_id: r.reject_reason for r in rejected.collect()} == {
        "T2": "ORPHAN_ACCOUNT", "T3": "INVALID_SIDE", "T4": "MISSING_ACCOUNT",
        "T5": "MALFORMED_RECORD", "T6": "UNKNOWN_SECURITY"}


def test_row_hash_distinguishes_null_and_shifted_values(spark):
    df = spark.createDataFrame([("ab", "c"), ("a", "bc"), (None, ""), ("", None)], "x STRING, y STRING")
    assert df.select(row_hash("x", "y").alias("h")).distinct().count() == 4


def test_stage_scd2_emits_close_and_insert_rows(spark):
    current = spark.createDataFrame([(1, "h1"), (2, "h2")], "account_id BIGINT, _row_hash STRING")
    incoming = spark.createDataFrame([(1, "h1"), (2, "h2-changed"), (3, "h3")], "account_id BIGINT, _row_hash STRING")
    staged = stage_scd2(incoming, current, "account_id")
    expected = spark.createDataFrame(
        [(1, "h1", 1), (2, "h2-changed", 2), (3, "h3", 3), (2, "h2-changed", None)],
        "account_id BIGINT, _row_hash STRING, merge_key BIGINT")
    # Order-insensitive comparison (pyspark.testing.assertDataFrameEqual does the same
    # but needs pandas, which the cluster image does not ship).
    key = lambda r: (r.account_id, r.merge_key is None)  # noqa: E731
    assert sorted(staged.collect(), key=key) == sorted(expected.collect(), key=key)
    assert staged.schema == expected.schema
