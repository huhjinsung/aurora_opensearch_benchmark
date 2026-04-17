#!/usr/bin/env python3
"""
시나리오 1 변형: 자계값 비정상 건수 COUNT (LIMIT 제거)
"""
import time
import json
import statistics
import pymysql
from opensearchpy import OpenSearch

MYSQL_ARGS = dict(
    host="tmoney-poc-aurora.cluster-cdm4w6mw8zge.ap-northeast-2.rds.amazonaws.com",
    user="admin",
    password="TmoneyPoC2026!",
    database="poc_tmoney",
    autocommit=True,
    read_timeout=600,
    write_timeout=600,
    connect_timeout=30,
)

OS_HOST = "vpc-tmoney-poc-os-dbsyzp4esz6r2zyswnfhknibxa.ap-northeast-2.es.amazonaws.com"
OS_USER = "admin"
OS_PASS = "TmoneyPoC2026!"

MYSQL_SQL = """
    SELECT COUNT(*)
    FROM tgls_param_logs
    WHERE CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) < -1500
       OR CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) > 0
"""

OS_BODY = {
    "size": 0,
    "track_total_hits": True,
    "query": {
        "bool": {
            "should": [
                {"range": {"mag_max_val": {"lt": -1500}}},
                {"range": {"mag_max_val": {"gt": 0}}}
            ],
            "minimum_should_match": 1
        }
    }
}


def run_mysql(sql):
    conn = pymysql.connect(**MYSQL_ARGS)
    try:
        t0 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
        return elapsed, result
    finally:
        conn.close()


def run_os(body):
    client = OpenSearch(
        hosts=[{"host": OS_HOST, "port": 443}],
        http_auth=(OS_USER, OS_PASS),
        use_ssl=True, verify_certs=True, ssl_show_warn=False, timeout=120,
    )
    t0 = time.perf_counter()
    resp = client.search(index="tgls-param-logs", body=body)
    elapsed = (time.perf_counter() - t0) * 1000
    total_hits = resp["hits"]["total"]["value"]
    return elapsed, total_hits


def main():
    print("=== 시나리오 1 변형: 자계값 비정상 건수 COUNT ===", flush=True)
    print("    LIMIT 100 제거 → COUNT(*) 전체 집계\n", flush=True)

    # MySQL
    print("[MySQL] cold run...", flush=True)
    mysql_cold, mysql_result = run_mysql(MYSQL_SQL)
    print(f"  cold: {mysql_cold:.1f}ms  (결과: {mysql_result})", flush=True)

    # OpenSearch
    print(f"\n[OpenSearch] cold run...", flush=True)
    os_cold, os_hits = run_os(OS_BODY)
    print(f"  cold: {os_cold:.1f}ms  (결과: {os_hits}건)", flush=True)

    # Summary
    speedup = mysql_cold / os_cold if os_cold > 0 else 0

    print(f"\n{'='*50}", flush=True)
    print(f"[결과 비교]", flush=True)
    print(f"  MySQL:      {mysql_cold:.1f}ms", flush=True)
    print(f"  OpenSearch: {os_cold:.1f}ms", flush=True)
    print(f"  성능 배수: {speedup:.1f}x (OpenSearch가 빠름)", flush=True)

    result = {
        "name": "시나리오 1 변형: 자계값 비정상 COUNT (cold only)",
        "mysql_cold_ms": round(mysql_cold, 1),
        "os_cold_ms": round(os_cold, 1),
        "speedup": round(speedup, 1),
    }
    outfile = "/home/ssm-user/benchmark/result_v2_s1_count.json"
    with open(outfile, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] {outfile}", flush=True)


if __name__ == "__main__":
    main()
