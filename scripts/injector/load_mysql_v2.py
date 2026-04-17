#!/usr/bin/env python3
"""
Aurora MySQL 데이터 적재 (v2 - 태그리스 파라미터로그)

generate_tgls_data.py로 생성한 .jsonl 파일을 읽어서
고객 현행 방식(VARCHAR에 JSON/Base64 통째로)으로 INSERT합니다.

사용법:
  python3 load_mysql_v2.py \
    --host AURORA_ENDPOINT --user admin --password PASSWORD \
    --input tgls_10m.jsonl
"""

import argparse
import json
import time
import pymysql


BATCH_SIZE = 1000
DATABASE = "poc_tmoney"
TABLE = "tgls_param_logs"

INSERT_SQL = f"""INSERT INTO {TABLE}
    (tgls_prmt_log_val, tgls_loc_log_larg_ctt, tgls_evnt_log_larg_ctt,
     tgls_prmt_flag_val, station_id, device_id, rgt_dtm, moapp_trns_trd_trnc_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""


def main():
    parser = argparse.ArgumentParser(description="Aurora MySQL 데이터 적재 (v2)")
    parser.add_argument("--host", required=True)
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

    print(f"[INFO] Aurora MySQL 적재 시작", flush=True)
    print(f"  - host: {args.host}", flush=True)
    print(f"  - input: {args.input}", flush=True)
    print(f"  - batch_size: {BATCH_SIZE}", flush=True)

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

                    record = json.loads(line)

                    # param_log dict → JSON 문자열로 VARCHAR에 저장
                    param_json = json.dumps(record["param_log"], ensure_ascii=False)

                    row = (
                        param_json,
                        record["loc_log"],
                        record["event_log"],
                        record.get("flag_val"),
                        record["station_id"],
                        record["device_id"],
                        record["rgt_dtm"],
                        record["transaction_id"],
                    )
                    batch.append(row)

                    if len(batch) >= BATCH_SIZE:
                        cursor.executemany(INSERT_SQL, batch)
                        conn.commit()
                        total += len(batch)
                        batch = []

                        if total % 100_000 == 0:
                            elapsed = time.time() - t0
                            rate = total / elapsed
                            print(f"  [{total:>12,}] {elapsed:.1f}s ({rate:,.0f} rows/s)", flush=True)

            if batch:
                cursor.executemany(INSERT_SQL, batch)
                conn.commit()
                total += len(batch)

    finally:
        conn.close()

    elapsed = time.time() - t0
    print(f"\n[OK] MySQL 적재 완료", flush=True)
    print(f"  - 총 건수: {total:,}", flush=True)
    print(f"  - 소요 시간: {elapsed:.1f}초", flush=True)
    print(f"  - 처리 속도: {total/elapsed:,.0f} rows/s", flush=True)


if __name__ == "__main__":
    main()
