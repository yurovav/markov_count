from collections import defaultdict
import argparse
import sqlite3
import sys
import os

try:
    import psycopg2
except Exception:
    psycopg2 = None

RUS_LETTERS = list("абвгдеёжзийклмнопрстуфхцчшщъыьэюя")
ALLOWED = set(RUS_LETTERS + ['.', ',', ' ', '!', '?'])
MAX_PREFIX = 13


def normalize_text(s):
    s = s.lower()
    return ''.join(ch for ch in s if ch in ALLOWED)


def collect_counts(text, max_prefix):
    counts = defaultdict(int)
    unigram = defaultdict(int)
    n = len(text)
    for i in range(n):
        c = text[i]
        unigram[c] += 1
        for k in range(1, max_prefix + 1):
            if i - k < 0:
                break
            prefix = text[i - k:i]
            counts[(prefix, c, k)] += 1
    return counts, unigram


def init_sqlite(conn):
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS counts (prefix TEXT, next_char TEXT, length INTEGER, count INTEGER, PRIMARY KEY(prefix, next_char, length))"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS unigram (char TEXT PRIMARY KEY, count INTEGER)"
    )
    conn.commit()


def upsert_sqlite(conn, counts, unigram):
    cur = conn.cursor()
    for (prefix, c, k), v in counts.items():
        cur.execute(
            "INSERT INTO counts VALUES (?, ?, ?, ?) ON CONFLICT(prefix, next_char, length) DO UPDATE SET count = count + ?",
            (prefix, c, k, v, v),
        )
    for c, v in unigram.items():
        cur.execute(
            "INSERT INTO unigram VALUES (?, ?) ON CONFLICT(char) DO UPDATE SET count = count + ?",
            (c, v, v),
        )
    conn.commit()


def init_postgres(conn):
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS counts (prefix TEXT, next_char TEXT, length INTEGER, count BIGINT, PRIMARY KEY(prefix, next_char, length))"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS unigram (char TEXT PRIMARY KEY, count BIGINT)"
    )
    conn.commit()


def upsert_postgres(conn, counts, unigram):
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO counts VALUES (%s,%s,%s,%s) ON CONFLICT (prefix,next_char,length) DO UPDATE SET count = counts.count + EXCLUDED.count",
        [(p, c, k, v) for (p, c, k), v in counts.items()],
    )
    cur.executemany(
        "INSERT INTO unigram VALUES (%s,%s) ON CONFLICT (char) DO UPDATE SET count = unigram.count + EXCLUDED.count",
        [(c, v) for c, v in unigram.items()],
    )
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--db", default="sqlite:///markov.db")
    ap.add_argument("--max-prefix", type=int, default=13)
    args = ap.parse_args()

    if args.max_prefix > 13:
        args.max_prefix = 13

    if not os.path.exists(args.corpus):
        sys.exit(1)

    with open(args.corpus, encoding="utf-8") as f:
        text = normalize_text(f.read())

    counts, unigram = collect_counts(text, args.max_prefix)

    if args.db.startswith("sqlite:///"):
        path = args.db.replace("sqlite:///", "")
        conn = sqlite3.connect(path)
        init_sqlite(conn)
        upsert_sqlite(conn, counts, unigram)
        conn.close()
    elif args.db.startswith("postgresql://"):
        if psycopg2 is None:
            sys.exit(1)
        conn = psycopg2.connect(args.db)
        init_postgres(conn)
        upsert_postgres(conn, counts, unigram)
        conn.close()
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
