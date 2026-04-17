#!/usr/bin/env python3
"""
OpenSearch 데이터 적재 (v2 - 태그리스 파라미터로그)

generate_tgls_data.py로 생성한 .jsonl 파일을 읽어서
JSON 필드를 네이티브 타입으로 파싱하여 적재합니다.

사전 조건:
  python3 setup_opensearch_v2.py create --host ENDPOINT --user admin --password PASSWORD

사용법:
  python3 load_opensearch_v2.py \
    --host OPENSEARCH_ENDPOINT --user admin --password PASSWORD \
    --input tgls_10m.jsonl

적재 완료 후:
  python3 setup_opensearch_v2.py finalize --host ENDPOINT --user admin --password PASSWORD
"""

import argparse
import json
import time
from opensearchpy import OpenSearch, helpers


INDEX_NAME = "tgls-param-logs"
BULK_SIZE = 2000


def transform_record(record):
    """
    JSONL 레코드를 OpenSearch 문서로 변환.
    파라미터로그 JSON을 파싱하여 네이티브 타입 필드로 분리.
    위치로그에서 추출한 자계 점수 통계를 수치 필드로 저장.
    """
    param = record["param_log"]
    mag_scores = record.get("loc_mag_scores", [])

    doc = {
        # 메타 필드
        "station_id": record["station_id"],
        "station_name": record["station_name"],
        "device_id": record["device_id"],
        "transaction_id": record["transaction_id"],
        "rgt_dtm": record["rgt_dtm"],

        # 파라미터로그 → 네이티브 타입
        "arrival_time": int(param["a"]) if param.get("a") else None,
        "initial_zone": param.get("b"),
        "open_door_zone": param.get("c"),
        "mag_max_val": int(param["d"]) if param.get("d") else None,
        "open_door_time": int(param["e"]) if param.get("e") else None,
        "ble_signal_f": param.get("f"),
        "ble_signal_g": param.get("g"),
        "ble_signal_h": param.get("h"),
        "ble_signal_i": param.get("i"),
        "ble_signal_j": param.get("j"),
        "ble_signal_k": param.get("k"),
        "mag_secondary_val": int(param["l"]) if param.get("l") else None,
        "ble_signal_m": param.get("m"),
        "ble_signal_n": param.get("n"),
        "ble_signal_o": param.get("o") if param.get("o") else None,
        "ble_signal_p": param.get("p") if param.get("p") else None,
        "transaction_type": param.get("t"),
        "device_status": param.get("u"),

        # 위치로그 — 파싱 결과 수치 저장
        "loc_log_raw": record["loc_log"],
        "loc_mag_avg": record.get("loc_mag_avg"),
        "loc_mag_min": min(mag_scores) if mag_scores else None,
        "loc_mag_max": max(mag_scores) if mag_scores else None,

        # 이벤트로그 원본
        "event_log_raw": record["event_log"],

        # 플래그
        "flag_val": record.get("flag_val"),

        # 파라미터로그 원본 (보존용)
        "prmt_log_raw": json.dumps(param, ensure_ascii=False),
    }

    return doc


def doc_generator(filepath, index_name):
    """JSONL → OpenSearch bulk 문서 제너레이터"""
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            doc = transform_record(record)
            yield {
                "_index": index_name,
                "_source": doc,
            }


def main():
    parser = argparse.ArgumentParser(description="OpenSearch 데이터 적재 (v2)")
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", required=True)
    parser.add_argument("--input", required=True, help=".jsonl 파일 경로")
    parser.add_argument("--bulk-size", type=int, default=BULK_SIZE)
    args = parser.parse_args()

    client = OpenSearch(
        hosts=[{"host": args.host, "port": 443}],
        http_auth=(args.user, args.password),
        use_ssl=True,
        verify_certs=True,
        ssl_show_warn=False,
    )

    if not client.indices.exists(index=INDEX_NAME):
        print(f"[ERROR] 인덱스 '{INDEX_NAME}'가 존재하지 않습니다.", flush=True)
        print(f"  먼저: python3 setup_opensearch_v2.py create ...", flush=True)
        return

    print(f"[INFO] OpenSearch 적재 시작", flush=True)
    print(f"  - host: {args.host}", flush=True)
    print(f"  - index: {INDEX_NAME}", flush=True)
    print(f"  - input: {args.input}", flush=True)
    print(f"  - bulk_size: {args.bulk_size}", flush=True)

    t0 = time.time()
    total = 0
    errors = 0
    batch = []

    for action in doc_generator(args.input, INDEX_NAME):
        batch.append(action)

        if len(batch) >= args.bulk_size:
            success, failed = helpers.bulk(
                client, batch, raise_on_error=False,
                raise_on_exception=False,
            )
            total += success
            errors += (len(batch) - success)
            batch = []

            if total % 100_000 < args.bulk_size:
                elapsed = time.time() - t0
                rate = total / elapsed
                print(f"  [{total:>12,}] {elapsed:.1f}s ({rate:,.0f} docs/s)", flush=True)

    if batch:
        success, failed = helpers.bulk(
            client, batch, raise_on_error=False,
            raise_on_exception=False,
        )
        total += success
        errors += (len(batch) - success)

    elapsed = time.time() - t0
    print(f"\n[OK] OpenSearch 적재 완료", flush=True)
    print(f"  - 성공: {total:,}", flush=True)
    print(f"  - 실패: {errors:,}", flush=True)
    print(f"  - 소요 시간: {elapsed:.1f}초", flush=True)
    print(f"  - 처리 속도: {total/elapsed:,.0f} docs/s", flush=True)
    print(f"\n[NEXT] 운영 설정 복원:", flush=True)
    print(f"  python3 setup_opensearch_v2.py finalize --host {args.host} --user {args.user} --password ****", flush=True)


if __name__ == "__main__":
    main()
