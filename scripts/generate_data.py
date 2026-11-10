#!/usr/bin/env python3
"""
Synthetic wealth-management data for the WM end-of-day batch.

Standard library only, deterministic (fixed seed), so every run produces the same
files. Writes:

  sql/mysql/init/02_seed.sql            CRM / OLTP seed (branch, FA, party, account)
  sql/mysql/changes/20261109_changes.sql  day-2 CRM changes (FA reassignment, new account)
  nas/inbound/<date>/                     custodian + middle-office feeds per business date
      trades_YYYYMMDD.csv                 trade blotter (corrections, cancels, bad rows)
      cash_activity_YYYYMMDD.psv          pipe-delimited cash movements, MM/dd/yyyy dates
      corporate_actions_YYYYMMDD.json     JSON Lines, nested "terms" struct
      custodian_positions_YYYYMMDD.csv    custodian's view of holdings (with breaks)
      _SUCCESS                            end-of-feed marker the Airflow sensor waits for
  data/market/                            vendor market data (goes to S3, or HDFS fallback)
      security_master/security_master.csv
      prices/price_date=YYYY-MM-DD/prices.json
  data/kafka/intraday_trades_YYYYMMDD.jsonl  OMS trade events for the Kafka producer

Business dates: 2026-11-06 (Fri) is the initial history load, then 2026-11-09 (Mon)
and 2026-11-10 (Tue) are normal daily batches. SEC00001 splits 2-for-1 on 2026-11-10.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import random
import shutil
from pathlib import Path

SEED = 42
HISTORY_START = dt.date(2023, 1, 3)
INITIAL_DATE = dt.date(2026, 11, 6)
DAILY_DATES = [dt.date(2026, 11, 9), dt.date(2026, 11, 10)]
ALL_BATCH_DATES = [INITIAL_DATE, *DAILY_DATES]
SPLIT_SECURITY, SPLIT_EX_DATE, SPLIT_RATIO = "SEC00001", dt.date(2026, 11, 10), 2
T1_SETTLEMENT_START = dt.date(2024, 5, 28)  # US moved from T+2 to T+1 on this date
CRM_BASELINE_TS = "2026-11-01 08:00:00"
CRM_CHANGE_TS = "2026-11-09 17:30:00"
ORPHAN_ACCOUNT_ID = 5999999
US_HOLIDAYS = {
    dt.date(2023, 1, 16), dt.date(2023, 2, 20), dt.date(2023, 4, 7), dt.date(2023, 5, 29),
    dt.date(2023, 6, 19), dt.date(2023, 7, 4), dt.date(2023, 9, 4), dt.date(2023, 11, 23),
    dt.date(2023, 12, 25), dt.date(2024, 1, 1), dt.date(2024, 1, 15), dt.date(2024, 2, 19),
    dt.date(2024, 3, 29), dt.date(2024, 5, 27), dt.date(2024, 6, 19), dt.date(2024, 7, 4),
    dt.date(2024, 9, 2), dt.date(2024, 11, 28), dt.date(2024, 12, 25), dt.date(2025, 1, 1),
    dt.date(2025, 1, 20), dt.date(2025, 2, 17), dt.date(2025, 4, 18), dt.date(2025, 5, 26),
    dt.date(2025, 6, 19), dt.date(2025, 7, 4), dt.date(2025, 9, 1), dt.date(2025, 11, 27),
    dt.date(2025, 12, 25), dt.date(2026, 1, 1), dt.date(2026, 1, 19), dt.date(2026, 2, 16),
    dt.date(2026, 4, 3), dt.date(2026, 5, 25), dt.date(2026, 6, 19), dt.date(2026, 7, 3),
    dt.date(2026, 9, 7),
}

FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
               "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
               "Thomas", "Sarah", "Priya", "Arjun", "Wei", "Mei", "Carlos", "Sofia", "Ahmed",
               "Fatima", "Kenji", "Yuki", "Olga", "Ivan", "Grace", "Daniel", "Emily", "Ravi"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Patel", "Shah", "Chen", "Wang", "Kim", "Nguyen", "Lee",
              "Walker", "Hall", "Allen", "Young", "King", "Wright", "Lopez", "Hill", "Scott",
              "Green", "Adams", "Baker", "Nelson", "Carter", "Mitchell", "Reddy", "Iyer"]
ORG_SUFFIX = ["Family Trust", "Foundation", "Holdings LLC", "Revocable Trust", "Partners LP"]
SECTORS = ["Technology", "Healthcare", "Financials", "Energy", "Industrials", "Consumer",
           "Utilities", "Materials", "Real Estate", "Communication"]
BRANCHES = [
    (101, "Park Avenue", "New York", "NY", "NORTHEAST"),
    (102, "Back Bay", "Boston", "MA", "NORTHEAST"),
    (103, "Buckhead", "Atlanta", "GA", "SOUTHEAST"),
    (104, "Brickell", "Miami", "FL", "SOUTHEAST"),
    (105, "Loop", "Chicago", "IL", "MIDWEST"),
    (106, "Uptown", "Dallas", "TX", "MIDWEST"),
    (107, "Century City", "Los Angeles", "CA", "WEST"),
    (108, "Financial District", "San Francisco", "CA", "WEST"),
]
STATES = ["NY", "NJ", "CT", "MA", "GA", "FL", "IL", "TX", "CA", "WA", "CO", "AZ"]


# ----------------------------------------------------------------------------- helpers
def is_business_day(d: dt.date) -> bool:
    return d.weekday() < 5 and d not in US_HOLIDAYS


def business_days(start: dt.date, end: dt.date) -> list[dt.date]:
    out, d = [], start
    while d <= end:
        if is_business_day(d):
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def add_business_days(d: dt.date, n: int) -> dt.date:
    while n:
        d += dt.timedelta(days=1)
        if is_business_day(d):
            n -= 1
    return d


def sql_str(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def cusip_for(n: int) -> str:
    return hashlib.sha1(f"cusip{n}".encode()).hexdigest()[:9].upper()


def money(x: float) -> str:
    return f"{x:.2f}"


# ----------------------------------------------------------------------------- reference data
def build_reference(rng: random.Random):
    advisors = []
    for i in range(40):
        fa_id = 9001 + i
        branch = BRANCHES[i % len(BRANCHES)]
        if fa_id == 9001:
            first, last, title = "Private Wealth", "Group", "Private Wealth Team"
        else:
            first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
            title = rng.choice(["Financial Advisor", "Senior Vice President", "Wealth Advisor",
                                "First Vice President"])
        advisors.append(dict(
            fa_id=fa_id, fa_code=f"FA{fa_id}", first_name=first, last_name=last,
            branch_id=branch[0], title=title,
            hire_date=(dt.date(2005, 1, 1) + dt.timedelta(days=rng.randint(0, 6000))).isoformat(),
            status="ACTIVE", email=f"fa{fa_id}@wm-firm.example",
            updated_at=CRM_BASELINE_TS))

    parties, accounts = [], []
    account_id = 5000001
    for i in range(1500):
        party_id = 100001 + i
        is_org = rng.random() < 0.1
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        parties.append(dict(
            party_id=party_id, party_type="ORG" if is_org else "IND",
            first_name=None if is_org else first, last_name=None if is_org else last,
            org_name=f"{last} {rng.choice(ORG_SUFFIX)}" if is_org else None,
            date_of_birth=None if is_org else (dt.date(1940, 1, 1) + dt.timedelta(days=rng.randint(0, 22000))).isoformat(),
            tax_id_last4=f"{rng.randint(0, 9999):04d}",
            risk_profile=rng.choice(["CONSERVATIVE", "MODERATE", "MODERATE", "GROWTH", "AGGRESSIVE"]),
            kyc_status="PENDING" if rng.random() < 0.02 else "VERIFIED",
            country="US", state=rng.choice(STATES),
            email=f"client{party_id}@mail.example",
            created_at="2015-01-01 00:00:00", updated_at=CRM_BASELINE_TS))
        n_accts = 1 if rng.random() < 0.55 else (2 if rng.random() < 0.8 else 3)
        for _ in range(n_accts):
            if is_org:
                acct_type = rng.choice(["TRUST", "TRUST", "BROKERAGE"])
            else:
                acct_type = rng.choices(["BROKERAGE", "IRA", "ROTH_IRA", "ADVISORY"], [45, 22, 12, 21])[0]
            tax_status = {"IRA": "TAX_DEFERRED", "ROTH_IRA": "TAX_EXEMPT"}.get(acct_type, "TAXABLE")
            open_date = dt.date(2012, 1, 1) + dt.timedelta(days=rng.randint(0, 3600))
            accounts.append(dict(
                account_id=account_id, account_number=f"WM{account_id:08d}", party_id=party_id,
                account_type=acct_type, tax_status=tax_status,
                fa_id=rng.choice(advisors[1:])["fa_id"], open_date=open_date.isoformat(),
                close_date=None, status="ACTIVE", base_currency="USD", cost_basis_method="FIFO",
                created_at=f"{open_date.isoformat()} 09:00:00", updated_at=CRM_BASELINE_TS,
                heavy=False))
            account_id += 1

    # A Private Wealth team that owns a few very active trust accounts: this is the
    # data-skew case (one key holding a large share of the rows).
    trusts = [a for a in accounts if a["account_type"] == "TRUST"]
    for a in trusts[:25]:
        a["fa_id"] = 9001
        a["heavy"] = True
    # A handful of closed accounts that no longer trade.
    for a in rng.sample([a for a in accounts if not a["heavy"]], 20):
        a["status"] = "CLOSED"
        a["close_date"] = "2022-12-30"

    securities = []
    plan = [("EQUITY", 60), ("ETF", 30), ("FIXED_INCOME", 15), ("MUTUAL_FUND", 15)]
    n = 1
    used_tickers = set()
    for asset_class, count in plan:
        for _ in range(count):
            while True:
                ticker = "".join(rng.choice("ABCDEFGHIJKLMNOPRSTUVWXZ") for _ in range(rng.choice([3, 4])))
                if ticker not in used_tickers:
                    used_tickers.add(ticker)
                    break
            if asset_class == "FIXED_INCOME":
                name = f"{rng.choice(STATES)} Municipal Bond {rng.randint(2028, 2045)}"
                start_price, tax_exempt, sector = rng.uniform(95, 105), True, "Municipal"
            elif asset_class == "MUTUAL_FUND":
                name = f"{ticker} {rng.choice(['Growth', 'Value', 'Income', 'Balanced'])} Fund"
                start_price, tax_exempt, sector = rng.uniform(10, 60), False, "Multi-Asset"
            elif asset_class == "ETF":
                name = f"{ticker} {rng.choice(['S&P 500', 'Total Market', 'Sector', 'Dividend'])} ETF"
                start_price, tax_exempt, sector = rng.uniform(50, 450), False, rng.choice(SECTORS)
            else:
                name = f"{ticker} {rng.choice(['Corp', 'Inc', 'Holdings', 'Group'])}"
                start_price, tax_exempt, sector = rng.uniform(20, 400), False, rng.choice(SECTORS)
            securities.append(dict(
                security_id=f"SEC{n:05d}", cusip=cusip_for(n), ticker=ticker, security_name=name,
                asset_class=asset_class, sector=sector, currency="USD", is_tax_exempt=tax_exempt,
                issuer=name.split(" ")[0], start_price=start_price,
                pays_income=asset_class == "FIXED_INCOME" or (asset_class != "MUTUAL_FUND" and rng.random() < 0.5)))
            n += 1
    return advisors, parties, accounts, securities


def build_prices(rng: random.Random, securities, days):
    prices: dict[tuple[str, dt.date], float] = {}
    for s in securities:
        p = s["start_price"]
        vol = {"EQUITY": 0.018, "ETF": 0.011, "FIXED_INCOME": 0.002, "MUTUAL_FUND": 0.008}[s["asset_class"]]
        drift = 0.0003 if s["asset_class"] in ("EQUITY", "ETF") else 0.00005
        for d in days:
            p = max(1.0, p * (1 + rng.gauss(drift, vol)))
            if s["security_id"] == SPLIT_SECURITY and d == SPLIT_EX_DATE:
                p = p / SPLIT_RATIO
            prices[(s["security_id"], d)] = round(p, 4)
    return prices


# ----------------------------------------------------------------------------- simulation
class Book:
    """Holdings that the generator keeps in step with the trades it emits."""

    def __init__(self):
        self.qty: dict[tuple[int, str], int] = {}

    def held(self, account_id: int):
        return [(k[1], q) for k, q in self.qty.items() if k[0] == account_id and q > 0]

    def apply(self, account_id, security_id, side, qty):
        k = (account_id, security_id)
        self.qty[k] = self.qty.get(k, 0) + (qty if side == "BUY" else -qty)
        if self.qty[k] < 0:
            raise AssertionError(f"oversold {k}")

    def split(self, security_id, ratio):
        for k in list(self.qty):
            if k[1] == security_id:
                self.qty[k] *= ratio


def make_trade(rng, seq, d, account, security, side, qty, price):
    notional = qty * price
    commission = 0.0 if account["account_type"] == "ADVISORY" else round(max(4.95, notional * 0.0005), 2)
    net = -(notional + commission) if side == "BUY" else notional - commission
    settle = add_business_days(d, 1 if d >= T1_SETTLEMENT_START else 2)
    exec_ts = dt.datetime.combine(d, dt.time(14, 30)) + dt.timedelta(seconds=rng.randint(0, 23400))
    return dict(
        trade_id=f"T{d:%Y%m%d}{seq:05d}", account_id=account["account_id"],
        security_id=security, trade_date=d.isoformat(), settle_date=settle.isoformat(),
        side=side, quantity=qty, price=f"{price:.4f}", commission=money(commission),
        net_amount=money(net), trade_status="NEW", version=1, source_system="OMS",
        exec_ts=exec_ts.strftime("%Y-%m-%d %H:%M:%S"))


def simulate_day(rng, d, n_trades, trading_accounts, weights, securities, prices, book):
    trades = []
    sec_ids = [s["security_id"] for s in securities]
    for seq in range(1, n_trades + 1):
        account = rng.choices(trading_accounts, weights)[0]
        held = book.held(account["account_id"])
        if held and rng.random() < 0.38:
            security, have = rng.choice(held)
            qty = have if rng.random() < 0.3 else max(1, int(have * rng.uniform(0.1, 0.7)))
            side = "SELL"
        else:
            security = rng.choice(sec_ids)
            close = prices[(security, d)]
            qty = max(1, int(rng.uniform(2_000, 60_000) / close))
            side = "BUY"
        price = round(prices[(security, d)] * (1 + rng.uniform(-0.004, 0.004)), 4)
        t = make_trade(rng, seq, d, account, security, side, qty, price)
        book.apply(account["account_id"], security, side, qty)
        trades.append(t)
    return trades


# ----------------------------------------------------------------------------- writers
TRADE_COLS = ["trade_id", "account_id", "security_id", "trade_date", "settle_date", "side",
              "quantity", "price", "commission", "net_amount", "trade_status", "version",
              "source_system", "exec_ts"]


def write_trades_file(path: Path, rows, bad_lines):
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(TRADE_COLS)
        for r in rows:
            w.writerow([r[c] for c in TRADE_COLS])
        for line in bad_lines:
            f.write(line + "\n")


def corrections_and_cancels(rng, d, trades, n_corr, n_cancel, accounts_by_id, securities):
    """Return (file_rows, stream_rows) for one day.

    file  = what the custodian/middle office sends at end of day
    stream = intraday OMS events (always version 1, may contain redelivered duplicates)
    """
    file_rows, stream_rows = [], []
    corrected = set(t["trade_id"] for t in rng.sample(trades, min(n_corr, len(trades))))
    for t in trades:
        if t["trade_id"] in corrected:
            original = dict(t, price=f"{float(t['price']) * 0.985:.4f}")
            original["net_amount"] = money(
                (-1 if t["side"] == "BUY" else 1) * float(original["price"]) * t["quantity"]
                + (-1 * float(t["commission"])))
            final = dict(t, trade_status="CORRECT", version=2)
            file_rows += [original, final]
            stream_rows.append(original)
        else:
            file_rows.append(t)
            stream_rows.append(t)
    # Trades booked then cancelled the same day: never reach the books.
    for i in range(n_cancel):
        acct = rng.choice(list(accounts_by_id.values()))
        sec = rng.choice(securities)["security_id"]
        t = make_trade(rng, 90000 + i, d, acct, sec, "BUY", rng.randint(10, 200), 100.0)
        file_rows += [t, dict(t, trade_status="CANCEL", version=2)]
        stream_rows.append(t)
    return file_rows, stream_rows


def bad_trade_lines(rng, d, sample_trades, n_malformed, n_null_acct, n_orphan, n_dupes):
    lines = []
    for i in range(n_malformed):
        lines.append(f"T{d:%Y%m%d}8{i:04d},5000010,SEC00010,{d},{d},BUY,10,abc,4.95,-1004.95,NEW,1,OMS,{d} 15:00:00")
    for i in range(n_null_acct):
        lines.append(f"T{d:%Y%m%d}7{i:04d},,SEC00011,{d},{d},BUY,10,50.0000,4.95,-504.95,NEW,1,OMS,{d} 15:01:00")
    for i in range(n_orphan):
        lines.append(f"T{d:%Y%m%d}6{i:04d},{ORPHAN_ACCOUNT_ID},SEC00012,{d},{d},BUY,5,80.0000,4.95,-404.95,NEW,1,OMS,{d} 15:02:00")
    for t in rng.sample(sample_trades, n_dupes):
        lines.append(",".join(str(t[c]) for c in TRADE_COLS))
    return lines


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    ap.add_argument("--history-trades-per-day", type=int, default=22)
    ap.add_argument("--daily-trades", type=int, default=300)
    args = ap.parse_args()

    root = Path(args.root)
    rng = random.Random(SEED)
    advisors, parties, accounts, securities = build_reference(rng)
    accounts_by_id = {a["account_id"]: a for a in accounts}
    sec_by_id = {s["security_id"]: s for s in securities}

    # The new account opened on day 2 (only trades from 2026-11-10).
    new_party_id = 100001 + len(parties)
    new_account = dict(
        account_id=accounts[-1]["account_id"] + 1, account_number=f"WM{accounts[-1]['account_id'] + 1:08d}",
        party_id=new_party_id, account_type="BROKERAGE", tax_status="TAXABLE", fa_id=9005,
        open_date="2026-11-09", close_date=None, status="ACTIVE", base_currency="USD",
        cost_basis_method="FIFO", created_at=CRM_CHANGE_TS, updated_at=CRM_CHANGE_TS, heavy=False)

    all_days = business_days(HISTORY_START, DAILY_DATES[-1])
    prices = build_prices(rng, securities, all_days)

    trading = [a for a in accounts if a["status"] == "ACTIVE"]
    weights = [15 if a["heavy"] else 1 for a in trading]
    book = Book()

    # ---------------------------------------------------------------- history (initial load)
    history_trades, cash_rows = [], []
    txn_seq = 1

    def cash(d, account_id, txn_type, amount, security_id=None, desc=""):
        nonlocal txn_seq
        cash_rows.append(dict(
            txn_id=f"C{d:%Y%m%d}{txn_seq:06d}", account_id=account_id, txn_date=d.strftime("%m/%d/%Y"),
            txn_type=txn_type, amount=money(amount), currency="USD", security_id=security_id or "",
            description=desc, _date=d))
        txn_seq += 1

    for a in trading:
        cash(HISTORY_START, a["account_id"], "DEPOSIT",
             rng.uniform(1_500_000, 6_000_000) if a["heavy"] else rng.uniform(100_000, 900_000),
             desc="Initial funding")

    history_days = [d for d in all_days if d <= INITIAL_DATE]
    for d in history_days:
        history_trades += simulate_day(rng, d, args.history_trades_per_day, trading, weights,
                                       securities, prices, book)
        # Quarterly income on the first business day of Feb/May/Aug/Nov.
        if d.month in (2, 5, 8, 11) and d.day <= 3 and d == min(x for x in history_days if x.month == d.month and x.year == d.year):
            for (acct, sec), q in list(book.qty.items()):
                s = sec_by_id[sec]
                if q > 0 and s["pays_income"]:
                    kind = "INTEREST" if s["asset_class"] == "FIXED_INCOME" else "DIVIDEND"
                    rate = 0.009 if kind == "INTEREST" else 0.004
                    cash(d, acct, kind, q * prices[(sec, d)] * rate, sec, f"{kind.title()} {s['ticker']}")
        if d.day <= 3 and d == min(x for x in history_days if x.month == d.month and x.year == d.year):
            for a in trading:
                if a["account_type"] == "ADVISORY":
                    cash(d, a["account_id"], "FEE", -rng.uniform(150, 900), desc="Advisory fee")

    init_dir = root / "nas" / "inbound" / INITIAL_DATE.isoformat()
    daily_out = {INITIAL_DATE: dict(trades=history_trades, cash=cash_rows)}

    # ---------------------------------------------------------------- daily batches
    custodian_snapshots = {INITIAL_DATE: dict(book.qty)}
    for d in DAILY_DATES:
        if d == SPLIT_EX_DATE:
            book.split(SPLIT_SECURITY, SPLIT_RATIO)
        day_accounts, day_weights = trading, weights
        if d >= dt.date(2026, 11, 10):
            day_accounts = trading + [new_account]
            day_weights = weights + [40]
        trades = simulate_day(rng, d, args.daily_trades, day_accounts, day_weights, securities, prices, book)
        start = len(cash_rows)
        for _ in range(25):
            a = rng.choice(trading)
            kind = rng.choice(["DEPOSIT", "WITHDRAWAL"])
            cash(d, a["account_id"], kind, rng.uniform(5_000, 80_000) * (1 if kind == "DEPOSIT" else -1),
                 desc=f"ACH {kind.lower()}")
        if d == dt.date(2026, 11, 10):
            cash(d, new_account["account_id"], "DEPOSIT", 750_000, desc="Initial funding")
        daily_out[d] = dict(trades=trades, cash=cash_rows[start:])
        custodian_snapshots[d] = dict(book.qty)

    # The initial-load cash file holds everything up to the initial date.
    daily_out[INITIAL_DATE]["cash"] = [c for c in cash_rows if c["_date"] <= INITIAL_DATE]

    # ---------------------------------------------------------------- write NAS feeds
    nas_root = root / "nas" / "inbound"
    if nas_root.exists():
        shutil.rmtree(nas_root)
    kafka_dir = root / "data" / "kafka"
    kafka_dir.mkdir(parents=True, exist_ok=True)

    all_accounts_by_id = dict(accounts_by_id)
    all_accounts_by_id[new_account["account_id"]] = new_account

    for d in ALL_BATCH_DATES:
        out = nas_root / d.isoformat()
        out.mkdir(parents=True, exist_ok=True)
        ymd = f"{d:%Y%m%d}"
        day_trades = daily_out[d]["trades"]
        todays = [t for t in day_trades if t["trade_date"] == d.isoformat()]
        n_corr, n_cancel = (2, 1) if d == INITIAL_DATE else (6, 4)
        corr_file, stream_rows = corrections_and_cancels(rng, d, todays, n_corr, n_cancel,
                                                         {k: v for k, v in all_accounts_by_id.items() if v["status"] == "ACTIVE"},
                                                         securities)
        older = [t for t in day_trades if t["trade_date"] != d.isoformat()]
        file_rows = older + corr_file
        bad = bad_trade_lines(rng, d, todays, *((2, 1, 1, 2) if d == INITIAL_DATE else (3, 2, 2, 4)))
        write_trades_file(out / f"trades_{ymd}.csv", file_rows, bad)

        # Intraday OMS events -> Kafka (daily dates only; the initial load has no stream).
        if d != INITIAL_DATE:
            with (kafka_dir / f"intraday_trades_{ymd}.jsonl").open("w") as f:
                events = stream_rows + rng.sample(stream_rows, 2)  # at-least-once redelivery
                for t in events:
                    ev = {k: t[k] for k in TRADE_COLS}
                    ev["quantity"] = int(ev["quantity"])
                    ev["price"] = float(ev["price"])
                    ev["commission"] = float(ev["commission"])
                    ev["net_amount"] = float(ev["net_amount"])
                    ev["event_ts"] = t["exec_ts"]
                    f.write(f"{t['account_id']}|{json.dumps(ev)}\n")

        with (out / f"cash_activity_{ymd}.psv").open("w") as f:
            f.write("txn_id|account_id|txn_date|txn_type|amount|currency|security_id|description\n")
            for c in daily_out[d]["cash"]:
                f.write("|".join(str(c[k]) for k in ["txn_id", "account_id", "txn_date", "txn_type",
                                                    "amount", "currency", "security_id", "description"]) + "\n")
            f.write(f"C{ymd}BAD001|5000020|13/45/2026|DEPOSIT|100.00|USD||Bad date row\n")

        with (out / f"corporate_actions_{ymd}.json").open("w") as f:
            events = []
            if d == INITIAL_DATE:
                events.append(dict(ca_id="CA2026000001", security_id=SPLIT_SECURITY, event_type="SPLIT",
                                   ex_date=SPLIT_EX_DATE.isoformat(), pay_date=SPLIT_EX_DATE.isoformat(),
                                   terms=dict(ratio_new=SPLIT_RATIO, ratio_old=1, cash_rate=None),
                                   announced_ts="2026-10-20 13:00:00"))
                for i, s in enumerate([s for s in securities if s["pays_income"] and s["asset_class"] != "FIXED_INCOME"][:12]):
                    events.append(dict(ca_id=f"CA2026{100 + i:06d}", security_id=s["security_id"],
                                       event_type="CASH_DIVIDEND", ex_date="2026-10-30", pay_date="2026-11-02",
                                       terms=dict(ratio_new=None, ratio_old=None, cash_rate=round(rng.uniform(0.1, 1.2), 4)),
                                       announced_ts="2026-10-15 12:00:00"))
            else:
                s = rng.choice(securities)
                events.append(dict(ca_id=f"CA2026{900 + d.day:06d}", security_id=s["security_id"],
                                   event_type="CASH_DIVIDEND", ex_date=(d + dt.timedelta(days=14)).isoformat(),
                                   pay_date=(d + dt.timedelta(days=21)).isoformat(),
                                   terms=dict(ratio_new=None, ratio_old=None, cash_rate=0.35),
                                   announced_ts=f"{d} 12:00:00"))
            for e in events:
                f.write(json.dumps(e) + "\n")
            f.write('{"ca_id": "CA_BROKEN", "security_id": "SEC00002", "event_type": \n')  # malformed

        with (out / f"custodian_positions_{ymd}.csv").open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["as_of_date", "custodian_account", "security_cusip", "quantity", "custodian_name"])
            snap = {k: v for k, v in custodian_snapshots[d].items() if v > 0}
            keys = sorted(snap)
            breaks = set(rng.sample(keys, 3))
            missing = rng.choice([k for k in keys if k not in breaks])
            for k in keys:
                if k == missing:
                    continue
                q = snap[k] + (rng.choice([-25, 10, 100]) if k in breaks else 0)
                acct = all_accounts_by_id[k[0]]
                w.writerow([d.isoformat(), acct["account_number"], sec_by_id[k[1]]["cusip"], q, "CUSTODIAN_BANK_A"])
            extra = accounts[7]
            w.writerow([d.isoformat(), extra["account_number"], sec_by_id["SEC00120"]["cusip"], 42, "CUSTODIAN_BANK_A"])

        (out / "_SUCCESS").write_text("")

    # ---------------------------------------------------------------- market data (S3 / HDFS)
    market = root / "data" / "market"
    if market.exists():
        shutil.rmtree(market)
    (market / "security_master").mkdir(parents=True)
    with (market / "security_master" / "security_master.csv").open("w", newline="") as f:
        cols = ["security_id", "cusip", "ticker", "security_name", "asset_class", "sector",
                "currency", "is_tax_exempt", "issuer"]
        w = csv.writer(f)
        w.writerow(cols)
        for s in securities:
            w.writerow([str(s[c]).lower() if c == "is_tax_exempt" else s[c] for c in cols])
    price_days = [d for d in all_days if d >= dt.date(2026, 10, 26)]
    for d in price_days:
        p = market / "prices" / f"price_date={d.isoformat()}"
        p.mkdir(parents=True)
        with (p / "prices.json").open("w") as f:
            for s in securities:
                f.write(json.dumps(dict(security_id=s["security_id"], close_price=prices[(s["security_id"], d)],
                                        currency="USD", source="VENDOR_EOD")) + "\n")

    # ---------------------------------------------------------------- MySQL seed + day-2 changes
    init_sql = root / "sql" / "mysql" / "init" / "02_seed.sql"
    init_sql.parent.mkdir(parents=True, exist_ok=True)

    def inserts(table, rows, cols):
        lines = []
        for i in range(0, len(rows), 500):
            chunk = rows[i:i + 500]
            values = ",\n".join("(" + ", ".join(sql_str(r[c]) for c in cols) + ")" for r in chunk)
            lines.append(f"INSERT INTO {table} ({', '.join(cols)}) VALUES\n{values};")
        return "\n".join(lines)

    branch_rows = [dict(branch_id=b[0], branch_name=b[1], city=b[2], state=b[3], region=b[4],
                        updated_at=CRM_BASELINE_TS) for b in BRANCHES]
    party_cols = ["party_id", "party_type", "first_name", "last_name", "org_name", "date_of_birth",
                  "tax_id_last4", "risk_profile", "kyc_status", "country", "state", "email",
                  "created_at", "updated_at"]
    acct_cols = ["account_id", "account_number", "party_id", "account_type", "tax_status", "fa_id",
                 "open_date", "close_date", "status", "base_currency", "cost_basis_method",
                 "created_at", "updated_at"]
    fa_cols = ["fa_id", "fa_code", "first_name", "last_name", "branch_id", "title", "hire_date",
               "status", "email", "updated_at"]
    with init_sql.open("w") as f:
        f.write("-- Generated by scripts/generate_data.py. Do not edit by hand.\nUSE wm_crm;\n\n")
        f.write(inserts("branch", branch_rows, ["branch_id", "branch_name", "city", "state", "region", "updated_at"]) + "\n\n")
        f.write(inserts("financial_advisor", advisors, fa_cols) + "\n\n")
        f.write(inserts("party", parties, party_cols) + "\n\n")
        f.write(inserts("account", accounts, acct_cols) + "\n")

    # Day-2 changes: FA 9040 leaves the firm and their book is reassigned; some clients
    # update their risk profile; a new client opens an account.
    leaving = 9040
    reassigned = [a for a in accounts if a["fa_id"] == leaving]
    risk_changes = rng.sample(parties, 10)
    changes = root / "sql" / "mysql" / "changes" / "20261109_changes.sql"
    changes.parent.mkdir(parents=True, exist_ok=True)
    with changes.open("w") as f:
        f.write("-- Day-2 CRM changes, applied with `make crm-changes` before the 2026-11-09 batch.\n")
        f.write("-- Drives SCD2 history in silver.dim_account and SCD1 updates in dim_party.\nUSE wm_crm;\n\n")
        f.write(f"UPDATE financial_advisor SET status = 'TERMINATED', updated_at = '{CRM_CHANGE_TS}' WHERE fa_id = {leaving};\n")
        for a in reassigned:
            f.write(f"UPDATE account SET fa_id = 9012, updated_at = '{CRM_CHANGE_TS}' WHERE account_id = {a['account_id']};\n")
        for p in risk_changes:
            new = "AGGRESSIVE" if p["risk_profile"] != "AGGRESSIVE" else "MODERATE"
            f.write(f"UPDATE party SET risk_profile = '{new}', updated_at = '{CRM_CHANGE_TS}' WHERE party_id = {p['party_id']};\n")
        f.write(inserts("party", [dict(
            party_id=new_party_id, party_type="IND", first_name="Ananya", last_name="Rao", org_name=None,
            date_of_birth="1985-04-12", tax_id_last4="4321", risk_profile="GROWTH", kyc_status="VERIFIED",
            country="US", state="NJ", email=f"client{new_party_id}@mail.example",
            created_at=CRM_CHANGE_TS, updated_at=CRM_CHANGE_TS)], party_cols) + "\n")
        f.write(inserts("account", [new_account], acct_cols) + "\n")

    n_hist = len(history_trades)
    print(f"branches={len(BRANCHES)} advisors={len(advisors)} parties={len(parties)} accounts={len(accounts)} "
          f"securities={len(securities)}")
    print(f"history trades={n_hist} cash rows={len(cash_rows)} reassigned accounts={len(reassigned)}")
    for d in ALL_BATCH_DATES:
        print(f"  {d}: trades file rows={sum(1 for _ in open(nas_root / d.isoformat() / f'trades_{d:%Y%m%d}.csv')) - 1}")
    print(f"wrote NAS feeds to {nas_root}, market data to {market}, MySQL seed to {init_sql}")


if __name__ == "__main__":
    main()
