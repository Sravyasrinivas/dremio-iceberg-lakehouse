"""
Generate synthetic card-issuing data for the local lakehouse demo.

Produces three raw tables as Parquet (a small "landing"/bronze zone):
  - accounts       : customer accounts
  - cards          : cards issued against accounts
  - transactions   : card transactions over ~90 days

Writes locally to data/output/ and, if MinIO env vars are present,
uploads the files to the MinIO `landing` bucket so Dremio can read them.

Usage:
    python data/generate_card_data.py                # local parquet only
    python data/generate_card_data.py --upload       # also push to MinIO

Nothing here is real customer data. Volumes are intentionally small so the
whole thing runs on a laptop in seconds.
"""
from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
N_ACCOUNTS = 500
CARDS_PER_ACCOUNT = (1, 3)          # min, max
N_TRANSACTIONS = 40_000
DAYS_BACK = 90

OUT_DIR = Path(__file__).parent / "output"

MERCHANT_CATEGORIES = [
    "GROCERY", "FUEL", "RESTAURANT", "TRAVEL", "ECOMMERCE",
    "ATM_WITHDRAWAL", "UTILITIES", "ENTERTAINMENT", "HEALTHCARE", "RETAIL",
]
CURRENCIES = ["CHF", "EUR", "USD", "GBP"]
CARD_STATUSES = ["ACTIVE", "ACTIVE", "ACTIVE", "BLOCKED", "EXPIRED"]  # weighted
CARD_NETWORKS = ["VISA", "MASTERCARD"]
TXN_STATUSES = ["APPROVED", "APPROVED", "APPROVED", "APPROVED", "DECLINED"]


def _rng():
    random.seed(SEED)
    np.random.seed(SEED)


def build_accounts() -> pd.DataFrame:
    countries = ["CH", "DE", "AT", "FR", "IT"]
    rows = []
    open_start = datetime(2019, 1, 1)
    for i in range(1, N_ACCOUNTS + 1):
        opened = open_start + timedelta(days=int(np.random.randint(0, 2200)))
        rows.append({
            "account_id": f"ACC{i:06d}",
            "customer_name": f"Customer {i:04d}",
            "country_code": random.choice(countries),
            "segment": random.choice(["RETAIL", "PREMIUM", "PRIVATE", "BUSINESS"]),
            "opened_date": opened.date(),
            "is_active": bool(np.random.rand() > 0.05),
        })
    return pd.DataFrame(rows)


def build_cards(accounts: pd.DataFrame) -> pd.DataFrame:
    rows = []
    card_seq = 1
    for acc in accounts.itertuples():
        n_cards = np.random.randint(CARDS_PER_ACCOUNT[0], CARDS_PER_ACCOUNT[1] + 1)
        for _ in range(n_cards):
            issued = datetime(2021, 1, 1) + timedelta(days=int(np.random.randint(0, 1500)))
            expiry = issued + timedelta(days=365 * 4)
            rows.append({
                "card_id": f"CARD{card_seq:07d}",
                "account_id": acc.account_id,
                # masked PAN only — never generate real card numbers
                "card_pan_masked": f"{random.choice(['4','5'])}xxx-xxxx-xxxx-{np.random.randint(1000,9999)}",
                "network": random.choice(CARD_NETWORKS),
                "card_status": random.choice(CARD_STATUSES),
                "issued_date": issued.date(),
                "expiry_date": expiry.date(),
                "credit_limit": int(np.random.choice([2000, 5000, 10000, 20000, 50000])),
            })
            card_seq += 1
    return pd.DataFrame(rows)


def build_transactions(cards: pd.DataFrame) -> pd.DataFrame:
    card_ids = cards["card_id"].to_numpy()
    now = datetime(2026, 8, 1)
    start = now - timedelta(days=DAYS_BACK)
    span_seconds = int((now - start).total_seconds())

    idx = np.random.randint(0, len(card_ids), size=N_TRANSACTIONS)
    offsets = np.random.randint(0, span_seconds, size=N_TRANSACTIONS)

    rows = []
    for n in range(N_TRANSACTIONS):
        ts = start + timedelta(seconds=int(offsets[n]))
        category = random.choice(MERCHANT_CATEGORIES)
        # amounts vary by category, log-normal-ish
        base = {
            "ATM_WITHDRAWAL": 200, "TRAVEL": 400, "ECOMMERCE": 120,
            "GROCERY": 60, "FUEL": 80, "RESTAURANT": 45,
        }.get(category, 90)
        amount = round(float(np.random.gamma(2.0, base / 2.0)) + 1, 2)
        rows.append({
            "transaction_id": f"TXN{n:09d}",
            "card_id": card_ids[idx[n]],
            "txn_timestamp": ts,
            "amount": amount,
            "currency": random.choices(CURRENCIES, weights=[6, 3, 2, 1])[0],
            "merchant_category": category,
            "merchant_name": f"{category.title()} Merchant {np.random.randint(1, 300)}",
            "txn_status": random.choice(TXN_STATUSES),
            "is_international": bool(np.random.rand() < 0.15),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("txn_timestamp").reset_index(drop=True)


def write_local(frames: dict[str, pd.DataFrame]) -> list[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, df in frames.items():
        p = OUT_DIR / f"{name}.parquet"
        df.to_parquet(p, index=False)
        paths.append(p)
        print(f"  wrote {p}  ({len(df):,} rows)")
    return paths


def upload_to_minio(paths: list[Path]) -> None:
    import boto3
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    bucket = os.environ.get("MINIO_LANDING_BUCKET", "landing")
    key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    s3 = boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=key, aws_secret_access_key=secret,
        region_name="us-east-1",
    )
    for p in paths:
        table = p.stem
        s3_key = f"{table}/{p.name}"
        s3.upload_file(str(p), bucket, s3_key)
        print(f"  uploaded s3://{bucket}/{s3_key}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--upload", action="store_true", help="push parquet to MinIO landing bucket")
    args = ap.parse_args()

    _rng()
    print("Generating synthetic card data...")
    accounts = build_accounts()
    cards = build_cards(accounts)
    transactions = build_transactions(cards)

    frames = {"accounts": accounts, "cards": cards, "transactions": transactions}
    paths = write_local(frames)

    if args.upload:
        print("Uploading to MinIO...")
        upload_to_minio(paths)

    print("Done.")


if __name__ == "__main__":
    main()
