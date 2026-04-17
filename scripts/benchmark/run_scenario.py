#!/usr/bin/env python3
"""
개별 시나리오 실행기
Usage: python3 run_scenario.py <scenario_id> [warm_iterations]
"""
import sys
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

SCENARIOS = {
    1: {
        "name": "시간대별 이벤트 집계",
        "mysql": "SELECT HOUR(JSON_UNQUOTE(JSON_EXTRACT(log_data, '$.event_time'))) AS h, COUNT(*) AS cnt FROM device_event_logs GROUP BY h ORDER BY h",
        "opensearch": {"size": 0, "aggs": {"hourly": {"date_histogram": {"field": "event_time", "calendar_interval": "hour"}}}},
    },
    2: {
        "name": "특정 장비 이벤트 이력",
        "mysql": """SELECT log_data FROM device_event_logs WHERE JSON_EXTRACT(log_data, '$.device_id') = '"TM-BUS-00001"' ORDER BY created_at DESC LIMIT 100""",
        "opensearch": {"size": 100, "query": {"term": {"device_id": "TM-BUS-00001"}}, "sort": [{"event_time": "desc"}]},
    },
    3: {
        "name": "장비 오류 분석",
        "mysql": """SELECT JSON_UNQUOTE(JSON_EXTRACT(log_data, '$.error_code')) AS err, COUNT(*) AS cnt FROM device_event_logs WHERE JSON_EXTRACT(log_data, '$.event_type') = '"error"' GROUP BY err ORDER BY cnt DESC""",
        "opensearch": {"size": 0, "query": {"term": {"event_type": "error"}}, "aggs": {"error_codes": {"terms": {"field": "error_code", "size": 20}}}},
    },
    4: {
        "name": "위치 전문 검색",
        "mysql": "SELECT log_data FROM device_event_logs WHERE log_data LIKE '%강남%' LIMIT 50",
        "opensearch": {"size": 50, "query": {"match": {"location": "강남"}}},
    },
    5: {
        "name": "다중 조건 집계",
        "mysql": "SELECT JSON_UNQUOTE(JSON_EXTRACT(log_data, '$.device_type')) AS dtype, JSON_UNQUOTE(JSON_EXTRACT(log_data, '$.event_type')) AS etype, COUNT(*) AS cnt FROM device_event_logs GROUP BY dtype, etype ORDER BY dtype, cnt DESC",
        "opensearch": {"size": 0, "aggs": {"by_device_type": {"terms": {"field": "device_type", "size": 10}, "aggs": {"by_event_type": {"terms": {"field": "event_type", "size": 10}}}}}},
    },
    6: {
        "name": "최근 N분 이벤트",
        "mysql": "SELECT COUNT(*) FROM device_event_logs WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)",
        "opensearch": {"size": 0, "query": {"range": {"created_at": {"gte": "now-1d/d"}}}},
    },
    7: {
        "name": "장비 상태 모니터링",
        "mysql": "SELECT JSON_UNQUOTE(JSON_EXTRACT(log_data, '$.device_id')) AS did, COUNT(*) AS cnt FROM device_event_logs WHERE CAST(JSON_EXTRACT(log_data, '$.battery_level') AS UNSIGNED) <= 20 GROUP BY did ORDER BY cnt DESC LIMIT 20",
        "opensearch": {"size": 0, "query": {"range": {"battery_level": {"lte": 20}}}, "aggs": {"devices": {"terms": {"field": "device_id", "size": 20}}}},
    },
}


def run_mysql(sql):
    conn = pymysql.connect(**MYSQL_ARGS)
    try:
        t0 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.fetchall()
        return (time.perf_counter() - t0) * 1000
    finally:
        conn.close()


def run_os(body):
    client = OpenSearch(
        hosts=[{"host": OS_HOST, "port": 443}],
        http_auth=(OS_USER, OS_PASS),
        use_ssl=True, verify_certs=True, ssl_show_warn=False, timeout=120,
    )
    t0 = time.perf_counter()
    client.search(index="device-event-logs", body=body)
    return (time.perf_counter() - t0) * 1000


def main():
    scenario_id = int(sys.argv[1])
    warm_n = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    sc = SCENARIOS[scenario_id]
    print(f"=== 시나리오 {scenario_id}: {sc['name']} ===", flush=True)

    # MySQL
    print(f"[MySQL] cold run...", flush=True)
    mysql_cold = run_mysql(sc["mysql"])
    print(f"  cold: {mysql_cold:.1f}ms", flush=True)

    mysql_warm = []
    for i in range(warm_n):
        t = run_mysql(sc["mysql"])
        mysql_warm.append(t)
        print(f"  warm {i+1}: {t:.1f}ms", flush=True)

    # OpenSearch
    print(f"[OpenSearch] cold run...", flush=True)
    os_cold = run_os(sc["opensearch"])
    print(f"  cold: {os_cold:.1f}ms", flush=True)

    os_warm = []
    for i in range(warm_n):
        t = run_os(sc["opensearch"])
        os_warm.append(t)
        print(f"  warm {i+1}: {t:.1f}ms", flush=True)

    # Summary
    ms_avg = statistics.mean(mysql_warm)
    os_avg = statistics.mean(os_warm)
    speedup = ms_avg / os_avg if os_avg > 0 else 0

    result = {
        "id": scenario_id,
        "name": sc["name"],
        "mysql_cold_ms": round(mysql_cold, 1),
        "mysql_warm_avg_ms": round(ms_avg, 1),
        "mysql_warm_p50_ms": round(sorted(mysql_warm)[len(mysql_warm)//2], 1),
        "mysql_warm_all_ms": [round(t, 1) for t in mysql_warm],
        "os_cold_ms": round(os_cold, 1),
        "os_warm_avg_ms": round(os_avg, 1),
        "os_warm_p50_ms": round(sorted(os_warm)[len(os_warm)//2], 1),
        "os_warm_all_ms": [round(t, 1) for t in os_warm],
        "speedup": round(speedup, 1),
    }

    print(f"\n[결과]", flush=True)
    print(f"  MySQL:      cold={mysql_cold:.1f}ms  warm_avg={ms_avg:.1f}ms", flush=True)
    print(f"  OpenSearch: cold={os_cold:.1f}ms  warm_avg={os_avg:.1f}ms", flush=True)
    print(f"  배수: {speedup:.1f}x", flush=True)

    # JSON 저장
    outfile = f"/home/ssm-user/benchmark/result_s{scenario_id}.json"
    with open(outfile, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] {outfile}", flush=True)


if __name__ == "__main__":
    main()
