#!/usr/bin/env python3
"""
MySQL vs OpenSearch 성능 비교 벤치마크

7개 시나리오에 대해 양쪽 쿼리를 반복 실행하고 응답 시간을 측정합니다.
Cold run(첫 실행) / Warm run(캐시 적재 후) 구분.

사용법:
  python3 benchmark.py \
    --mysql-host AURORA_ENDPOINT \
    --mysql-user admin --mysql-password PASSWORD \
    --os-host OPENSEARCH_ENDPOINT \
    --os-user admin --os-password PASSWORD \
    --warm-iterations 3
"""

import argparse
import json
import statistics
import sys
import time
import pymysql
import pymysql.cursors
from opensearchpy import OpenSearch


# =============================================================================
# 시나리오 정의
# =============================================================================

def get_scenarios():
    """7개 시나리오 — MySQL 쿼리와 OpenSearch 쿼리 쌍"""
    return [
        {
            "id": 1,
            "name": "시간대별 이벤트 집계",
            "description": "30일간 시간대별 이벤트 건수 집계",
            "mysql": """
                SELECT HOUR(JSON_UNQUOTE(JSON_EXTRACT(log_data, '$.event_time'))) AS h,
                       COUNT(*) AS cnt
                FROM device_event_logs
                GROUP BY h
                ORDER BY h
            """,
            "opensearch": {
                "size": 0,
                "aggs": {
                    "hourly": {
                        "date_histogram": {
                            "field": "event_time",
                            "calendar_interval": "hour"
                        }
                    }
                }
            },
        },
        {
            "id": 2,
            "name": "특정 장비 이벤트 이력",
            "description": "장비 TM-BUS-00001의 최근 100건 이벤트",
            "mysql": """
                SELECT log_data
                FROM device_event_logs
                WHERE JSON_EXTRACT(log_data, '$.device_id') = '"TM-BUS-00001"'
                ORDER BY created_at DESC
                LIMIT 100
            """,
            "opensearch": {
                "size": 100,
                "query": {
                    "term": {"device_id": "TM-BUS-00001"}
                },
                "sort": [{"event_time": "desc"}],
            },
        },
        {
            "id": 3,
            "name": "장비 오류 분석",
            "description": "오류 이벤트의 error_code별 건수 집계",
            "mysql": """
                SELECT JSON_UNQUOTE(JSON_EXTRACT(log_data, '$.error_code')) AS err,
                       COUNT(*) AS cnt
                FROM device_event_logs
                WHERE JSON_EXTRACT(log_data, '$.event_type') = '"error"'
                GROUP BY err
                ORDER BY cnt DESC
            """,
            "opensearch": {
                "size": 0,
                "query": {
                    "term": {"event_type": "error"}
                },
                "aggs": {
                    "error_codes": {
                        "terms": {"field": "error_code", "size": 20}
                    }
                }
            },
        },
        {
            "id": 4,
            "name": "위치 전문 검색",
            "description": "'강남' 키워드가 포함된 이벤트 검색 (상위 50건)",
            "mysql": """
                SELECT log_data
                FROM device_event_logs
                WHERE log_data LIKE '%강남%'
                LIMIT 50
            """,
            "opensearch": {
                "size": 50,
                "query": {
                    "match": {"location": "강남"}
                }
            },
        },
        {
            "id": 5,
            "name": "다중 조건 집계",
            "description": "device_type별 + event_type별 건수 (multi-level aggregation)",
            "mysql": """
                SELECT JSON_UNQUOTE(JSON_EXTRACT(log_data, '$.device_type')) AS dtype,
                       JSON_UNQUOTE(JSON_EXTRACT(log_data, '$.event_type')) AS etype,
                       COUNT(*) AS cnt
                FROM device_event_logs
                GROUP BY dtype, etype
                ORDER BY dtype, cnt DESC
            """,
            "opensearch": {
                "size": 0,
                "aggs": {
                    "by_device_type": {
                        "terms": {"field": "device_type", "size": 10},
                        "aggs": {
                            "by_event_type": {
                                "terms": {"field": "event_type", "size": 10}
                            }
                        }
                    }
                }
            },
        },
        {
            "id": 6,
            "name": "최근 N분 이벤트",
            "description": "최근 5분간 발생한 이벤트 건수 (시뮬레이션: 최근 1일)",
            "mysql": """
                SELECT COUNT(*)
                FROM device_event_logs
                WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
            """,
            "opensearch": {
                "size": 0,
                "query": {
                    "range": {
                        "created_at": {"gte": "now-1d/d"}
                    }
                }
            },
        },
        {
            "id": 7,
            "name": "장비 상태 모니터링",
            "description": "배터리 20% 이하인 장비별 이벤트 수",
            "mysql": """
                SELECT JSON_UNQUOTE(JSON_EXTRACT(log_data, '$.device_id')) AS did,
                       COUNT(*) AS cnt
                FROM device_event_logs
                WHERE CAST(JSON_EXTRACT(log_data, '$.battery_level') AS UNSIGNED) <= 20
                GROUP BY did
                ORDER BY cnt DESC
                LIMIT 20
            """,
            "opensearch": {
                "size": 0,
                "query": {
                    "range": {"battery_level": {"lte": 20}}
                },
                "aggs": {
                    "devices": {
                        "terms": {"field": "device_id", "size": 20}
                    }
                }
            },
        },
    ]


# =============================================================================
# 벤치마크 실행
# =============================================================================

def make_mysql_conn(mysql_args):
    """MySQL 연결 생성 (매 쿼리마다 새 연결)"""
    return pymysql.connect(**mysql_args)


def run_mysql_query(mysql_args, sql):
    """MySQL 쿼리 실행 — 매번 새 연결로 타임아웃 방지"""
    conn = pymysql.connect(**mysql_args)
    try:
        t0 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.fetchall()
        elapsed = time.perf_counter() - t0
    finally:
        conn.close()
    return elapsed


def run_opensearch_query(client, body, index="device-event-logs"):
    """OpenSearch 쿼리 실행 후 응답 시간(초) 반환"""
    t0 = time.perf_counter()
    client.search(index=index, body=body)
    return time.perf_counter() - t0


def compute_stats(times):
    """통계 계산: avg, p50, p95, p99"""
    s = sorted(times)
    n = len(s)
    return {
        "avg": statistics.mean(s),
        "p50": s[n // 2],
        "p95": s[min(int(n * 0.95), n - 1)],
        "p99": s[min(int(n * 0.99), n - 1)],
        "min": s[0],
        "max": s[-1],
    }


def run_benchmark(mysql_args, os_client, warm_iterations):
    """전체 벤치마크 실행: 1회 cold run + N회 warm run"""
    scenarios = get_scenarios()
    results = []

    for sc in scenarios:
        print(f"\n{'='*60}", flush=True)
        print(f"시나리오 {sc['id']}: {sc['name']}", flush=True)
        print(f"  {sc['description']}", flush=True)
        print(f"{'='*60}", flush=True)

        # === MySQL ===
        # Cold run
        print(f"  MySQL cold run... ", end="", flush=True)
        try:
            mysql_cold = run_mysql_query(mysql_args, sc["mysql"])
            print(f"{mysql_cold*1000:.1f}ms", flush=True)
        except Exception as e:
            print(f"ERROR: {e}", flush=True)
            mysql_cold = None

        # Warm runs
        mysql_warm_times = []
        print(f"  MySQL warm runs ({warm_iterations}회): ", end="", flush=True)
        for i in range(warm_iterations):
            try:
                t = run_mysql_query(mysql_args, sc["mysql"])
                mysql_warm_times.append(t)
                print(".", end="", flush=True)
            except Exception as e:
                print(f" ERROR: {e}", flush=True)
                break
        print(f" {len(mysql_warm_times)}회 완료", flush=True)

        # === OpenSearch ===
        # Cold run
        print(f"  OS cold run... ", end="", flush=True)
        try:
            os_cold = run_opensearch_query(os_client, sc["opensearch"])
            print(f"{os_cold*1000:.1f}ms", flush=True)
        except Exception as e:
            print(f"ERROR: {e}", flush=True)
            os_cold = None

        # Warm runs
        os_warm_times = []
        print(f"  OS warm runs ({warm_iterations}회): ", end="", flush=True)
        for i in range(warm_iterations):
            try:
                t = run_opensearch_query(os_client, sc["opensearch"])
                os_warm_times.append(t)
                print(".", end="", flush=True)
            except Exception as e:
                print(f" ERROR: {e}", flush=True)
                break
        print(f" {len(os_warm_times)}회 완료", flush=True)

        # === 결과 집계 ===
        if mysql_warm_times and os_warm_times:
            ms = compute_stats(mysql_warm_times)
            os_s = compute_stats(os_warm_times)
            speedup = ms["avg"] / os_s["avg"] if os_s["avg"] > 0 else 0

            result = {
                "id": sc["id"],
                "name": sc["name"],
                "mysql_cold_ms": mysql_cold * 1000 if mysql_cold else None,
                "os_cold_ms": os_cold * 1000 if os_cold else None,
                "mysql_warm": {k: v * 1000 for k, v in ms.items()},
                "os_warm": {k: v * 1000 for k, v in os_s.items()},
                "speedup": speedup,
            }

            print(f"\n  결과 (warm avg):", flush=True)
            print(f"    MySQL      cold={result['mysql_cold_ms']:.1f}ms  warm_avg={result['mysql_warm']['avg']:.1f}ms  p50={result['mysql_warm']['p50']:.1f}ms  p95={result['mysql_warm']['p95']:.1f}ms", flush=True)
            print(f"    OpenSearch cold={result['os_cold_ms']:.1f}ms  warm_avg={result['os_warm']['avg']:.1f}ms  p50={result['os_warm']['p50']:.1f}ms  p95={result['os_warm']['p95']:.1f}ms", flush=True)
            print(f"    OpenSearch가 {speedup:.1f}배 빠름", flush=True)

            results.append(result)

    return results


def print_summary(results):
    """결과 요약 테이블 출력"""
    print(f"\n\n{'='*90}", flush=True)
    print(f"{'#':<3} {'시나리오':<22} {'MySQL cold':>12} {'MySQL warm':>12} {'OS cold':>10} {'OS warm':>10} {'배수':>8}", flush=True)
    print(f"{'='*90}", flush=True)
    for r in results:
        mc = f"{r['mysql_cold_ms']:.0f}ms" if r['mysql_cold_ms'] else "N/A"
        mw = f"{r['mysql_warm']['avg']:.0f}ms"
        oc = f"{r['os_cold_ms']:.0f}ms" if r['os_cold_ms'] else "N/A"
        ow = f"{r['os_warm']['avg']:.1f}ms"
        print(f"{r['id']:<3} {r['name']:<22} {mc:>12} {mw:>12} {oc:>10} {ow:>10} {r['speedup']:>7.1f}x", flush=True)
    print(f"{'='*90}", flush=True)

    avg_speedup = statistics.mean([r["speedup"] for r in results])
    print(f"{'평균 성능 배수':>61} {avg_speedup:>7.1f}x", flush=True)


def export_json(results, filepath):
    """결과를 JSON 파일로 저장"""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] 결과 저장: {filepath}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="MySQL vs OpenSearch 벤치마크")
    parser.add_argument("--mysql-host", required=True)
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-user", default="admin")
    parser.add_argument("--mysql-password", required=True)
    parser.add_argument("--os-host", required=True)
    parser.add_argument("--os-user", default="admin")
    parser.add_argument("--os-password", required=True)
    parser.add_argument("--warm-iterations", type=int, default=3, help="Warm run 반복 횟수")
    parser.add_argument("--output", default="benchmark_results.json", help="결과 JSON")
    args = parser.parse_args()

    mysql_args = dict(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database="poc_tmoney",
        autocommit=True,
        read_timeout=600,
        write_timeout=600,
        connect_timeout=30,
    )

    # MySQL 연결 테스트
    print("[INFO] MySQL 연결 테스트...", flush=True)
    conn = pymysql.connect(**mysql_args)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM device_event_logs")
        cnt = cur.fetchone()[0]
        print(f"  MySQL rows: {cnt:,}", flush=True)
    conn.close()

    # OpenSearch 연결
    print("[INFO] OpenSearch 연결 테스트...", flush=True)
    os_client = OpenSearch(
        hosts=[{"host": args.os_host, "port": 443}],
        http_auth=(args.os_user, args.os_password),
        use_ssl=True,
        verify_certs=True,
        ssl_show_warn=False,
        timeout=120,
    )
    os_cnt = os_client.count(index="device-event-logs")["count"]
    print(f"  OpenSearch docs: {os_cnt:,}", flush=True)

    print(f"\n[INFO] 벤치마크 시작 (cold 1회 + warm {args.warm_iterations}회)", flush=True)

    results = run_benchmark(mysql_args, os_client, args.warm_iterations)

    print_summary(results)
    export_json(results, args.output)


if __name__ == "__main__":
    main()
