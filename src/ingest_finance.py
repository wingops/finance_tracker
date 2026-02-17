import sqlite3
import pandas as pd
import uuid
import hashlib
import re
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

DB_PATH = "finance.sqlite"

# ----------------------------
# 1) Schema (v1)
# ----------------------------
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS accounts (
  account_id TEXT PRIMARY KEY,
  institution TEXT,
  name TEXT,
  type TEXT CHECK(type IN ('debit','credit')) NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
  category_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  parent_id INTEGER,
  FOREIGN KEY(parent_id) REFERENCES categories(category_id)
);

CREATE TABLE IF NOT EXISTS transactions (
  id TEXT PRIMARY KEY,

  txn_date TEXT NOT NULL,
  posted_date TEXT,

  amount_cents INTEGER NOT NULL,          -- cents-based
  currency TEXT NOT NULL DEFAULT 'USD',

  description_raw TEXT NOT NULL,
  description_clean TEXT NOT NULL,

  account_id TEXT NOT NULL,
  category_id INTEGER,

  dedupe_hash TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

  FOREIGN KEY(account_id) REFERENCES accounts(account_id),
  FOREIGN KEY(category_id) REFERENCES categories(category_id)
);

CREATE INDEX IF NOT EXISTS idx_txn_date ON transactions(txn_date);
CREATE INDEX IF NOT EXISTS idx_account_id ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_category_id ON transactions(category_id);

"""


def ensure_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.executescript(SCHEMA_SQL)
    con.commit()
    return con


# ----------------------------
# 2) Normalization helpers
# ----------------------------
def to_cents(x) -> int:
    """
    Convert dollars to integer cents exactly.
    Handles floats/strings; rounds half up to nearest cent.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        raise ValueError("Amount is missing")

    d = Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return int(d * 100)

NOISE_PHRASES = [
    "card purchase", "pos", "purchase", "debit", "credit", "us"
]

def clean_description(desc: str) -> str:
    if desc is None:
        return ""
    s = str(desc).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for n in NOISE_PHRASES:
        s = s.replace(n, "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def compute_dedupe_hash(txn_date: str, amount_cents: int, desc_clean: str, account_id: str) -> str:
    key = f"{txn_date}|{amount_cents}|{desc_clean}|{account_id}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def upsert_account(con: sqlite3.Connection, account_id: str, institution: str, name: str, acct_type: str):
    con.execute(
        """
        INSERT INTO accounts(account_id, institution, name, type)
        VALUES(?,?,?,?)
        ON CONFLICT(account_id) DO UPDATE SET
          institution=excluded.institution,
          name=excluded.name,
          type=excluded.type
        """,
        (account_id, institution, name, acct_type),
    )


# ----------------------------
# 3) Importers for your 2 sources
# ----------------------------
def ingest_source1_checking(con: sqlite3.Connection, csv_path: str, institution: str = "Santander"):
    """
    Source 1 columns (example):
    Date, ABA Num, Currency, Account Num, Account Name, Description, BAI Code, Amount, Serial Num, Ref Num
    """
    df = pd.read_csv(csv_path, sep=None, engine="python")  # handles tab or comma
    df.columns = [c.strip() for c in df.columns]

    # Create account from the first row (assuming one account per file)
    account_id = str(df.loc[0, "Account Num"]).strip()
    account_name = str(df.loc[0, "Account Name"]).strip()
    upsert_account(con, account_id, institution, account_name, "debit")

    # Normalize
    out = pd.DataFrame()
    out["txn_date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
    out["posted_date"] = out["txn_date"]
    out["amount"] = pd.to_numeric(df["Amount"])
    out["currency"] = df.get("Currency", "USD").fillna("USD")
    out["description_raw"] = df["Description"].astype(str)
    out["description_clean"] = out["description_raw"].apply(clean_description)
    out["account_id"] = account_id

    insert_transactions(con, out)


def ingest_source2_credit(con: sqlite3.Connection, csv_path: str, institution: str = "Capital One", card_name: str = "Chase Visa"):
    """
    Source 2 columns (example):
    Transaction Date, Posted Date, Card No., Description, Category, Debit, Credit
    """
    df = pd.read_csv(csv_path, sep=None, engine="python")
    df.columns = [c.strip() for c in df.columns]

    # One card per file assumption (reasonable for v1)
    account_id = str(df.loc[0, "Card No."]).strip()
    upsert_account(con, account_id, institution, card_name, "credit")

    debit = pd.to_numeric(df["Debit"], errors="coerce").fillna(0.0)
    credit = pd.to_numeric(df["Credit"], errors="coerce").fillna(0.0)

    # Convention: expenses are negative.
    # Many CC exports put charges in Debit and payments/refunds in Credit.
    amount = credit - debit

    out = pd.DataFrame()
    out["txn_date"] = pd.to_datetime(df["Transaction Date"]).dt.date.astype(str)
    out["posted_date"] = pd.to_datetime(df["Posted Date"], errors="coerce").dt.date.astype(str)
    out["amount"] = amount
    out["currency"] = "USD"
    out["description_raw"] = df["Description"].astype(str)
    out["description_clean"] = out["description_raw"].apply(clean_description)
    out["account_id"] = account_id

    insert_transactions(con, out)


# ----------------------------
# 4) Insert with dedupe
# ----------------------------
def insert_transactions(con: sqlite3.Connection, txns: pd.DataFrame):
    inserted = 0
    skipped = 0

    for row in txns.itertuples(index=False):
        txn_date = row.txn_date
        posted_date = row.posted_date if pd.notna(row.posted_date) else None
        amount_cents = to_cents(row.amount)
        currency = str(row.currency) if pd.notna(row.currency) else "USD"
        desc_raw = str(row.description_raw)
        desc_clean = str(row.description_clean)
        account_id = str(row.account_id)

        d_hash = compute_dedupe_hash(txn_date, amount_cents, desc_clean, account_id)

        try:
            con.execute(
                """
                INSERT INTO transactions(
                id, txn_date, posted_date, amount_cents, currency,
                description_raw, description_clean,
                account_id, category_id, dedupe_hash
                )
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(uuid.uuid4()),
                    txn_date,
                    posted_date,
                    amount_cents,
                    currency,
                    desc_raw,
                    desc_clean,
                    account_id,
                    None,
                    d_hash,
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            # most likely UNIQUE(dedupe_hash) hit
            skipped += 1

    con.commit()
    print(f"Inserted: {inserted}, Skipped (deduped): {skipped}")


# ----------------------------
# 5) Quick CLI entry point
# ----------------------------
def main():
    con = ensure_db(DB_PATH)

    # Example usage (edit paths):
    # ingest_source1_checking(con, "checking.tsv")
    # ingest_source2_credit(con, "credit.csv")

    con.close()


if __name__ == "__main__":
    main()
