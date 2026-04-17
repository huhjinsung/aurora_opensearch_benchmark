#!/usr/bin/env python3
"""
OpenSearch 인덱스 생성 및 관리 (v2 - 태그리스 파라미터로그)

사용법:
  python3 setup_opensearch_v2.py create --host ENDPOINT --user admin --password PASSWORD
  python3 setup_opensearch_v2.py finalize --host ENDPOINT --user admin --password PASSWORD
  python3 setup_opensearch_v2.py delete --host ENDPOINT --user admin --password PASSWORD
  python3 setup_opensearch_v2.py status --host ENDPOINT --user admin --password PASSWORD
"""

import argparse
import sys
from opensearchpy import OpenSearch


INDEX_NAME = "tgls-param-logs"

INDEX_BODY = {
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 0,
        "refresh_interval": "-1",
        "analysis": {
            "analyzer": {
                "korean_analyzer": {
                    "type": "custom",
                    "tokenizer": "seunjeon_default_tokenizer",
                    "filter": ["lowercase"]
                }
            },
            "tokenizer": {
                "seunjeon_default_tokenizer": {
                    "type": "seunjeon_tokenizer",
                    "index_eojeol": False,
                    "decompound": True
                }
            }
        }
    },
    "mappings": {
        "properties": {
            # 메타 필드
            "station_id":          {"type": "keyword"},
            "station_name":        {"type": "text", "analyzer": "korean_analyzer",
                                    "fields": {"raw": {"type": "keyword"}}},
            "device_id":           {"type": "keyword"},
            "transaction_id":      {"type": "keyword"},
            "rgt_dtm":             {"type": "date", "format": "yyyyMMddHHmmss"},

            # 파라미터로그 — 네이티브 타입으로 분리 저장
            "arrival_time":        {"type": "long"},
            "initial_zone":        {"type": "keyword"},
            "open_door_zone":      {"type": "keyword"},
            "mag_max_val":         {"type": "integer"},
            "open_door_time":      {"type": "long"},
            "ble_signal_f":        {"type": "keyword"},
            "ble_signal_g":        {"type": "keyword"},
            "ble_signal_h":        {"type": "keyword"},
            "ble_signal_i":        {"type": "keyword"},
            "ble_signal_j":        {"type": "keyword"},
            "ble_signal_k":        {"type": "keyword"},
            "mag_secondary_val":   {"type": "integer"},
            "ble_signal_m":        {"type": "keyword"},
            "ble_signal_n":        {"type": "keyword"},
            "ble_signal_o":        {"type": "keyword"},
            "ble_signal_p":        {"type": "keyword"},
            "transaction_type":    {"type": "keyword"},
            "device_status":       {"type": "keyword"},

            # 위치로그 — 바이트 파싱 결과를 수치로 저장
            "loc_log_raw":         {"type": "keyword", "index": False, "doc_values": False},
            "loc_mag_avg":         {"type": "float"},
            "loc_mag_min":         {"type": "integer"},
            "loc_mag_max":         {"type": "integer"},

            # 이벤트로그 원본
            "event_log_raw":       {"type": "keyword", "index": False, "doc_values": False},

            # 플래그
            "flag_val":            {"type": "integer"},

            # 파라미터로그 원본 (검색 불필요, 보존용)
            "prmt_log_raw":        {"type": "keyword", "index": False, "doc_values": False},
        }
    }
}

FINALIZE_SETTINGS = {
    "index": {
        "number_of_replicas": 1,
        "refresh_interval": "1s"
    }
}


def get_client(args):
    return OpenSearch(
        hosts=[{"host": args.host, "port": 443}],
        http_auth=(args.user, args.password),
        use_ssl=True,
        verify_certs=True,
        ssl_show_warn=False,
    )


def cmd_create(args):
    client = get_client(args)

    if client.indices.exists(index=INDEX_NAME):
        print(f"[WARN] 인덱스 '{INDEX_NAME}'가 이미 존재합니다.", flush=True)
        if args.force:
            client.indices.delete(index=INDEX_NAME)
            print(f"[INFO] 기존 인덱스 삭제 완료", flush=True)
        else:
            print(f"[INFO] --force 옵션으로 재생성하세요.", flush=True)
            sys.exit(1)

    client.indices.create(index=INDEX_NAME, body=INDEX_BODY)
    print(f"[OK] 인덱스 '{INDEX_NAME}' 생성 완료", flush=True)
    print(f"     - shards: 3, replicas: 0 (적재 최적화)", flush=True)
    print(f"     - refresh_interval: -1 (적재 최적화)", flush=True)
    print(f"     - seunjeon 한글 분석기: 활성화 (station_name)", flush=True)
    print(f"     - mag_max_val: integer, loc_mag_avg: float", flush=True)


def cmd_finalize(args):
    client = get_client(args)

    if not client.indices.exists(index=INDEX_NAME):
        print(f"[ERROR] 인덱스 '{INDEX_NAME}'가 존재하지 않습니다.", flush=True)
        sys.exit(1)

    client.indices.put_settings(index=INDEX_NAME, body=FINALIZE_SETTINGS)
    print(f"[OK] 운영 설정 복원: replicas=1, refresh=1s", flush=True)

    client.indices.refresh(index=INDEX_NAME)
    print(f"[OK] refresh 완료", flush=True)

    print(f"[INFO] force merge 실행 중...", flush=True)
    client.indices.forcemerge(index=INDEX_NAME, max_num_segments=1)
    print(f"[OK] force merge 완료", flush=True)

    _print_status(client)


def cmd_delete(args):
    client = get_client(args)
    if not client.indices.exists(index=INDEX_NAME):
        print(f"[INFO] 인덱스 '{INDEX_NAME}'가 존재하지 않습니다.", flush=True)
        return
    client.indices.delete(index=INDEX_NAME)
    print(f"[OK] 인덱스 '{INDEX_NAME}' 삭제 완료", flush=True)


def cmd_status(args):
    client = get_client(args)
    if not client.indices.exists(index=INDEX_NAME):
        print(f"[INFO] 인덱스 '{INDEX_NAME}'가 존재하지 않습니다.", flush=True)
        return
    _print_status(client)


def _print_status(client):
    settings = client.indices.get_settings(index=INDEX_NAME)
    idx_settings = settings[INDEX_NAME]["settings"]["index"]
    print(f"\n===== 인덱스 설정 =====", flush=True)
    print(f"  shards:           {idx_settings.get('number_of_shards')}", flush=True)
    print(f"  replicas:         {idx_settings.get('number_of_replicas')}", flush=True)
    print(f"  refresh_interval: {idx_settings.get('refresh_interval', '1s')}", flush=True)

    stats = client.indices.stats(index=INDEX_NAME)
    total = stats["_all"]["total"]
    print(f"\n===== 인덱스 통계 =====", flush=True)
    print(f"  문서 수:    {total['docs']['count']:,}", flush=True)
    print(f"  저장 크기:  {total['store']['size_in_bytes'] / (1024*1024):.1f} MB", flush=True)
    print(f"  세그먼트:   {total['segments']['count']}", flush=True)

    health = client.cluster.health(index=INDEX_NAME)
    print(f"\n===== 클러스터 헬스 =====", flush=True)
    print(f"  status:           {health['status']}", flush=True)
    print(f"  active_shards:    {health['active_shards']}", flush=True)
    print(f"  unassigned:       {health['unassigned_shards']}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="OpenSearch 인덱스 관리 (v2)")
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", required=True)

    subparsers = parser.add_subparsers(dest="command", required=True)

    p_create = subparsers.add_parser("create")
    p_create.add_argument("--force", action="store_true")

    subparsers.add_parser("finalize")
    subparsers.add_parser("delete")
    subparsers.add_parser("status")

    args = parser.parse_args()

    commands = {
        "create": cmd_create,
        "finalize": cmd_finalize,
        "delete": cmd_delete,
        "status": cmd_status,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
