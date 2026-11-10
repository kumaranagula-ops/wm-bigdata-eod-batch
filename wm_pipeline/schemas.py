"""Explicit schemas for every file feed (no inferSchema in production).

Money is DECIMAL, never DOUBLE. Each file schema ends with _corrupt_record so
PERMISSIVE mode keeps the raw text of rows that fail to parse, for quarantine.
"""
from pyspark.sql.types import (DateType, DecimalType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

QTY = DecimalType(18, 4)
PRICE = DecimalType(18, 6)
AMOUNT = DecimalType(18, 2)

TRADES_CSV = StructType([
    StructField("trade_id", StringType()),
    StructField("account_id", LongType()),
    StructField("security_id", StringType()),
    StructField("trade_date", DateType()),
    StructField("settle_date", DateType()),
    StructField("side", StringType()),
    StructField("quantity", QTY),
    StructField("price", PRICE),
    StructField("commission", AMOUNT),
    StructField("net_amount", AMOUNT),
    StructField("trade_status", StringType()),
    StructField("version", IntegerType()),
    StructField("source_system", StringType()),
    StructField("exec_ts", TimestampType()),
    StructField("_corrupt_record", StringType()),
])

CASH_PSV = StructType([
    StructField("txn_id", StringType()),
    StructField("account_id", LongType()),
    StructField("txn_date", DateType()),
    StructField("txn_type", StringType()),
    StructField("amount", AMOUNT),
    StructField("currency", StringType()),
    StructField("security_id", StringType()),
    StructField("description", StringType()),
    StructField("_corrupt_record", StringType()),
])

CORPORATE_ACTIONS_JSON = (
    "ca_id STRING, security_id STRING, event_type STRING, ex_date DATE, pay_date DATE, "
    "terms STRUCT<ratio_new: INT, ratio_old: INT, cash_rate: DECIMAL(18,6)>, "
    "announced_ts TIMESTAMP, _corrupt_record STRING"
)

CUSTODIAN_POSITIONS_CSV = StructType([
    StructField("as_of_date", DateType()),
    StructField("custodian_account", StringType()),
    StructField("security_cusip", StringType()),
    StructField("quantity", QTY),
    StructField("custodian_name", StringType()),
    StructField("_corrupt_record", StringType()),
])

SECURITY_MASTER_CSV = (
    "security_id STRING, cusip STRING, ticker STRING, security_name STRING, asset_class STRING, "
    "sector STRING, currency STRING, is_tax_exempt BOOLEAN, issuer STRING"
)

PRICES_JSON = "security_id STRING, close_price DECIMAL(18,6), currency STRING, source STRING"

# Kafka value payload: numbers arrive as JSON numbers, cast to DECIMAL after parsing.
TRADE_EVENT_JSON = (
    "trade_id STRING, account_id BIGINT, security_id STRING, trade_date DATE, settle_date DATE, "
    "side STRING, quantity DECIMAL(18,4), price DECIMAL(18,6), commission DECIMAL(18,2), "
    "net_amount DECIMAL(18,2), trade_status STRING, version INT, source_system STRING, "
    "exec_ts TIMESTAMP, event_ts TIMESTAMP"
)
