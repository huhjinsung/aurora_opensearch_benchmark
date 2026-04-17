#!/usr/bin/env python3
"""
Aurora MySQL 데이터 적재 스크립트

generate_data.py로 생성한 .jsonl 파일을 읽어서
고객 현행 방식(TEXT 컬럼에 JSON 통째로)으로 INSERT합니다.

사용법:
  python3 load_mysql.py \
    --host tmoney-poc-aurora.cluster-xxx.ap-northeast-2.rds.amazonaws.com \
    --user admin --password PASSWORD \
    --input data_10m.jsonl
"""

import argparse
import json
import time
import pymysql


BATCH_SIZE = 1000
DATABASE = "poc_tmoney"
TABLE = "device_event_logs"

INSERT_SQL = f"INSERT INTO {TABLE} (log_data, created_at) VALUES (%s, %s)"


def main():
    parser = argparse.ArgumentParser(description="Aurora MySQL 데이터 적재")
    parser.add_argument("--host", required=True, help="Aurora writer endpoint")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", required=True)
    parser.add_argument("--input", required=True, help=".jsonl 파일 경로")
    parser.add_argument("--database", default=DATABASE)
    args = parser.parse_args()

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        autocommit=False,
    )

    print(f"[INFO] Aurora MySQL 적재 시작")
    print(f"  - host: {args.host}")
    print(f"  - input: {args.input}")
    print(f"  - batch_size: {BATCH_SIZE}")

    t0 = time.time()
    total = 0
    batch = []

    try:
        with conn.cursor() as cursor:
            with open(args.input, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    event = json.loads(line)
                    created_at = event.get("created_at", event.get("event_time"))
                    batch.append((line, created_at))

                    if len(batch) >= BATCH_SIZE:
                        cursor.executemany(INSERT_SQL, batch)
                        conn.commit()
                        total += len(batch)
                        batch = []

                        if total % 100_000 == 0:
                            elapsed = time.time() - t0
                            rate = total / elapsed
                            print(f"  [{total:>12,}] {elapsed:.1f}s ({rate:,.0f} rows/s)")

            # 나머지
            if batch:
                cursor.executemany(INSERT_SQL, batch)
                conn.commit()
                total += len(batch)

    finally:
        conn.close()

    elapsed = time.time() - t0
    print(f"\n[OK] MySQL 적재 완료")
    print(f"  - 총 건수: {total:,}")
    print(f"  - 소요 시간: {elapsed:.1f}초")
    print(f"  - 처리 속도: {total/elapsed:,.0f} rows/s")


if __name__ == "__main__":
    main()
