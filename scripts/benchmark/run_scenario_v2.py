#!/usr/bin/env python3
"""
태그리스 파라미터로그 벤치마크 — 개별 시나리오 실행기
Usage: python3 run_scenario_v2.py <scenario_id> [warm_iterations]
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

# =============================================================================
# 8개 시나리오
# =============================================================================
SCENARIOS = {
    1: {
        "name": "자계값 범위 검색",
        "desc": "자계최대값이 비정상 범위(-1500 미만 또는 0 초과)인 거래 상위 100건 조회",
        "mysql": """
            SELECT tgls_prmt_log_val, station_id, device_id
            FROM tgls_param_logs
            WHERE CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) < -1500
               OR CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) > 0
            LIMIT 100
        """,
        "opensearch": {
            "size": 100,
            "query": {
                "bool": {
                    "should": [
                        {"range": {"mag_max_val": {"lt": -1500}}},
                        {"range": {"mag_max_val": {"gt": 0}}}
                    ],
                    "minimum_should_match": 1
                }
            }
        },
    },
    2: {
        "name": "자계 이상 + 위치로그 결합 분석",
        "desc": "자계최대값 비정상 + 위치로그 자계평균 50%(128) 이하 → 건수",
        "mysql": """
            SELECT COUNT(*)
            FROM tgls_param_logs
            WHERE (CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) < -1500
                   OR CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) > 0)
        """,
        "mysql_note": "MySQL에서는 위치로그(Base64) 파싱이 불가하여 자계값 조건만 적용. OpenSearch는 두 조건 모두 적용.",
        "opensearch": {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {"bool": {
                            "should": [
                                {"range": {"mag_max_val": {"lt": -1500}}},
                                {"range": {"mag_max_val": {"gt": 0}}}
                            ],
                            "minimum_should_match": 1
                        }},
                        {"range": {"loc_mag_avg": {"lte": 128}}}
                    ]
                }
            }
        },
    },
    3: {
        "name": "역사별/장치별 이상 거래 집계",
        "desc": "자계값 비정상인 거래를 역사ID × 장치ID별로 건수 집계 (상위 20)",
        "mysql": """
            SELECT station_id, device_id, COUNT(*) AS cnt
            FROM tgls_param_logs
            WHERE CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) < -1500
               OR CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) > 0
            GROUP BY station_id, device_id
            ORDER BY cnt DESC
            LIMIT 20
        """,
        "opensearch": {
            "size": 0,
            "query": {
                "bool": {
                    "should": [
                        {"range": {"mag_max_val": {"lt": -1500}}},
                        {"range": {"mag_max_val": {"gt": 0}}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "aggs": {
                "by_station": {
                    "terms": {"field": "station_id", "size": 20},
                    "aggs": {
                        "by_device": {
                            "terms": {"field": "device_id", "size": 20}
                        }
                    }
                }
            }
        },
    },
    4: {
        "name": "시간대별 거래 추이",
        "desc": "등록일시(rgt_dtm)의 시간(hour)별 거래 건수 집계",
        "mysql": """
            SELECT SUBSTR(rgt_dtm, 9, 2) AS h, COUNT(*) AS cnt
            FROM tgls_param_logs
            GROUP BY h
            ORDER BY h
        """,
        "opensearch": {
            "size": 0,
            "aggs": {
                "hourly": {
                    "date_histogram": {
                        "field": "rgt_dtm",
                        "calendar_interval": "hour"
                    }
                }
            }
        },
    },
    5: {
        "name": "zone별 IN/OUT 통계",
        "desc": "입장(I)/출장(O) zone별 거래 건수 분포",
        "mysql": """
            SELECT JSON_UNQUOTE(JSON_EXTRACT(tgls_prmt_log_val, '$.b')) AS zone_type,
                   COUNT(*) AS cnt
            FROM tgls_param_logs
            GROUP BY zone_type
        """,
        "opensearch": {
            "size": 0,
            "aggs": {
                "by_zone": {
                    "terms": {"field": "initial_zone", "size": 10}
                }
            }
        },
    },
    6: {
        "name": "장치 상태별 이상률",
        "desc": "장치 상태(u)별 전체 건수 및 자계 이상 건수",
        "mysql": """
            SELECT JSON_UNQUOTE(JSON_EXTRACT(tgls_prmt_log_val, '$.u')) AS device_status,
                   COUNT(*) AS total,
                   SUM(CASE WHEN CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) < -1500
                                 OR CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) > 0
                            THEN 1 ELSE 0 END) AS abnormal
            FROM tgls_param_logs
            GROUP BY device_status
        """,
        "opensearch": {
            "size": 0,
            "aggs": {
                "by_status": {
                    "terms": {"field": "device_status", "size": 10},
                    "aggs": {
                        "abnormal": {
                            "filter": {
                                "bool": {
                                    "should": [
                                        {"range": {"mag_max_val": {"lt": -1500}}},
                                        {"range": {"mag_max_val": {"gt": 0}}}
                                    ],
                                    "minimum_should_match": 1
                                }
                            }
                        }
                    }
                }
            }
        },
    },
    7: {
        "name": "특정 역사 거래 이력 조회",
        "desc": "역사 ST-001의 최근 100건 거래 이력",
        "mysql": """
            SELECT tgls_prmt_log_val, tgls_loc_log_larg_ctt, rgt_dtm
            FROM tgls_param_logs
            WHERE station_id = 'ST-001'
            ORDER BY rgt_dtm DESC
            LIMIT 100
        """,
        "opensearch": {
            "size": 100,
            "query": {
                "term": {"station_id": "ST-001"}
            },
            "sort": [{"rgt_dtm": "desc"}]
        },
    },
    8: {
        "name": "역사명 전문 검색",
        "desc": "'강남' 키워드로 역사명 검색 (한글 형태소 분석)",
        "mysql": """
            SELECT station_id, device_id, rgt_dtm, tgls_prmt_log_val
            FROM tgls_param_logs
            WHERE station_id IN (
                SELECT DISTINCT station_id
                FROM tgls_param_logs
                WHERE tgls_prmt_log_val LIKE '%강남%'
                   OR station_id IN (SELECT station_id FROM tgls_param_logs LIMIT 1)
            )
            LIMIT 50
        """,
        "mysql_alt": """
            SELECT tgls_prmt_log_val, station_id, rgt_dtm
            FROM tgls_param_logs
            WHERE tgls_prmt_log_val LIKE '%강남%'
            LIMIT 50
        """,
        "opensearch": {
            "size": 50,
            "query": {
                "match": {"station_name": "강남"}
            }
        },
    },
}

# 시나리오 8은 MySQL에서 station_name 필드가 없으므로 간소화된 LIKE 쿼리 사용
SCENARIOS[8]["mysql"] = SCENARIOS[8]["mysql_alt"]


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
    client.search(index="tgls-param-logs", body=body)
    return (time.perf_counter() - t0) * 1000


def main():
    scenario_id = int(sys.argv[1])
    warm_n = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    sc = SCENARIOS[scenario_id]
    print(f"=== 시나리오 {scenario_id}: {sc['name']} ===", flush=True)
    print(f"    {sc['desc']}", flush=True)
    if sc.get("mysql_note"):
        print(f"    [참고] {sc['mysql_note']}", flush=True)

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
        "desc": sc["desc"],
        "mysql_cold_ms": round(mysql_cold, 1),
        "mysql_warm_avg_ms": round(ms_avg, 1),
        "mysql_warm_all_ms": [round(t, 1) for t in mysql_warm],
        "os_cold_ms": round(os_cold, 1),
        "os_warm_avg_ms": round(os_avg, 1),
        "os_warm_all_ms": [round(t, 1) for t in os_warm],
        "speedup": round(speedup, 1),
    }
    if sc.get("mysql_note"):
        result["mysql_note"] = sc["mysql_note"]

    print(f"\n[결과]", flush=True)
    print(f"  MySQL:      cold={mysql_cold:.1f}ms  warm_avg={ms_avg:.1f}ms", flush=True)
    print(f"  OpenSearch: cold={os_cold:.1f}ms  warm_avg={os_avg:.1f}ms", flush=True)
    print(f"  배수: {speedup:.1f}x", flush=True)

    outfile = f"/home/ssm-user/benchmark/result_v2_s{scenario_id}.json"
    with open(outfile, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] {outfile}", flush=True)


if __name__ == "__main__":
    main()
