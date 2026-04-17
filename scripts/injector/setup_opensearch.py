#!/usr/bin/env python3
"""
OpenSearch 인덱스 생성 및 관리 스크립트

사용법:
  # 1. 인덱스 생성 (벌크 적재 최적화 설정 포함)
  python3 setup_opensearch.py create --host ENDPOINT --user admin --password PASSWORD

  # 2. 적재 완료 후 운영 설정으로 복원
  python3 setup_opensearch.py finalize --host ENDPOINT --user admin --password PASSWORD

  # 3. 인덱스 삭제 (재시작 시)
  python3 setup_opensearch.py delete --host ENDPOINT --user admin --password PASSWORD

  # 4. 인덱스 상태 확인
  python3 setup_opensearch.py status --host ENDPOINT --user admin --password PASSWORD
"""

import argparse
import json
import sys
from opensearchpy import OpenSearch


INDEX_NAME = "device-event-logs"

# =============================================================================
# 인덱스 매핑 정의
# =============================================================================
INDEX_BODY = {
    # ----- Settings -----
    "settings": {
        # Shard 구성: 데이터 노드 2대 기준
        # - 1,000만 건(~5GB): shard당 ~1.7GB → 가벼움
        # - 5,000만 건(~25GB): shard당 ~8.3GB → 적정 범위
        "number_of_shards": 3,

        # 벌크 적재 최적화: replica=0으로 시작 (적재 완료 후 1로 올림)
        # → 적재 중 복제 오버헤드 제거, 속도 2배 이상 향상
        "number_of_replicas": 0,

        # 벌크 적재 최적화: refresh 비활성화 (적재 완료 후 1s로 복원)
        # → 적재 중 세그먼트 머지 방지, I/O 감소
        "refresh_interval": "-1",

        # seunjeon 한글 형태소 분석기 설정 (AWS OpenSearch 기본 제공)
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

    # ----- Mappings -----
    "mappings": {
        "properties": {
            # 장비 식별
            "device_id": {
                "type": "keyword"
            },
            "device_type": {
                "type": "keyword"
            },

            # 이벤트 정보
            "event_type": {
                "type": "keyword"
            },
            "event_time": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            },

            # 위치 - 한글 전문 검색 지원 (seunjeon)
            "location": {
                "type": "text",
                "analyzer": "korean_analyzer",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },

            # 노선/카드
            "route_id": {
                "type": "keyword"
            },
            "card_number": {
                "type": "keyword"
            },

            # 요금
            "fare_amount": {
                "type": "integer"
            },

            # 상태
            "status": {
                "type": "keyword"
            },
            "error_code": {
                "type": "keyword"
            },

            # 장비 상태
            "firmware_ver": {
                "type": "keyword"
            },
            "battery_level": {
                "type": "integer"
            },

            # 지역
            "region_code": {
                "type": "keyword"
            },

            # 생성 시각
            "created_at": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            }
        }
    }
}

# 적재 완료 후 운영 설정
FINALIZE_SETTINGS = {
    "index": {
        "number_of_replicas": 1,
        "refresh_interval": "1s"
    }
}


def get_client(args):
    """OpenSearch 클라이언트 생성"""
    return OpenSearch(
        hosts=[{"host": args.host, "port": 443}],
        http_auth=(args.user, args.password),
        use_ssl=True,
        verify_certs=True,
        ssl_show_warn=False,
    )


def cmd_create(args):
    """인덱스 생성 (벌크 적재 최적화 설정)"""
    client = get_client(args)

    if client.indices.exists(index=INDEX_NAME):
        print(f"[WARN] 인덱스 '{INDEX_NAME}'가 이미 존재합니다.")
        if args.force:
            print(f"[INFO] --force 옵션으로 기존 인덱스 삭제 후 재생성합니다.")
            client.indices.delete(index=INDEX_NAME)
        else:
            print(f"[INFO] 기존 인덱스를 삭제하려면 --force 옵션을 사용하세요.")
            sys.exit(1)

    client.indices.create(index=INDEX_NAME, body=INDEX_BODY)
    print(f"[OK] 인덱스 '{INDEX_NAME}' 생성 완료")
    print(f"     - shards: {INDEX_BODY['settings']['number_of_shards']}")
    print(f"     - replicas: {INDEX_BODY['settings']['number_of_replicas']} (적재 최적화)")
    print(f"     - refresh_interval: {INDEX_BODY['settings']['refresh_interval']} (적재 최적화)")
    print(f"     - seunjeon 한글 분석기: 활성화")
    print()
    print("[NEXT] 데이터 적재 완료 후 아래 명령으로 운영 설정 복원:")
    print(f"  python3 setup_opensearch.py finalize --host {args.host} --user {args.user} --password ****")


def cmd_finalize(args):
    """적재 완료 후 운영 설정 복원: replica=1, refresh=1s, force merge"""
    client = get_client(args)

    if not client.indices.exists(index=INDEX_NAME):
        print(f"[ERROR] 인덱스 '{INDEX_NAME}'가 존재하지 않습니다.")
        sys.exit(1)

    # 1. replica=1, refresh_interval=1s 복원
    client.indices.put_settings(index=INDEX_NAME, body=FINALIZE_SETTINGS)
    print(f"[OK] 운영 설정 복원 완료")
    print(f"     - replicas: 1")
    print(f"     - refresh_interval: 1s")

    # 2. refresh 강제 실행
    client.indices.refresh(index=INDEX_NAME)
    print(f"[OK] refresh 완료")

    # 3. force merge (세그먼트 최적화, max 1 segment per shard)
    print(f"[INFO] force merge 실행 중 (시간 소요될 수 있음)...")
    client.indices.forcemerge(index=INDEX_NAME, max_num_segments=1)
    print(f"[OK] force merge 완료")

    # 4. 최종 상태 출력
    _print_status(client)


def cmd_delete(args):
    """인덱스 삭제"""
    client = get_client(args)

    if not client.indices.exists(index=INDEX_NAME):
        print(f"[INFO] 인덱스 '{INDEX_NAME}'가 존재하지 않습니다.")
        return

    client.indices.delete(index=INDEX_NAME)
    print(f"[OK] 인덱스 '{INDEX_NAME}' 삭제 완료")


def cmd_status(args):
    """인덱스 상태 확인"""
    client = get_client(args)

    if not client.indices.exists(index=INDEX_NAME):
        print(f"[INFO] 인덱스 '{INDEX_NAME}'가 존재하지 않습니다.")
        return

    _print_status(client)


def _print_status(client):
    """인덱스 상태 출력"""
    # settings
    settings = client.indices.get_settings(index=INDEX_NAME)
    idx_settings = settings[INDEX_NAME]["settings"]["index"]
    print(f"\n===== 인덱스 설정 =====")
    print(f"  shards:           {idx_settings.get('number_of_shards')}")
    print(f"  replicas:         {idx_settings.get('number_of_replicas')}")
    print(f"  refresh_interval: {idx_settings.get('refresh_interval', '1s')}")

    # stats
    stats = client.indices.stats(index=INDEX_NAME)
    total = stats["_all"]["total"]
    print(f"\n===== 인덱스 통계 =====")
    print(f"  문서 수:    {total['docs']['count']:,}")
    print(f"  저장 크기:  {total['store']['size_in_bytes'] / (1024*1024):.1f} MB")
    print(f"  세그먼트:   {total['segments']['count']}")

    # health
    health = client.cluster.health(index=INDEX_NAME)
    print(f"\n===== 클러스터 헬스 =====")
    print(f"  status:           {health['status']}")
    print(f"  active_shards:    {health['active_shards']}")
    print(f"  relocating:       {health['relocating_shards']}")
    print(f"  unassigned:       {health['unassigned_shards']}")


def main():
    parser = argparse.ArgumentParser(description="OpenSearch 인덱스 관리")
    parser.add_argument("--host", required=True, help="OpenSearch endpoint")
    parser.add_argument("--user", default="admin", help="Master username")
    parser.add_argument("--password", required=True, help="Master password")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # create
    p_create = subparsers.add_parser("create", help="인덱스 생성 (벌크 적재 최적화)")
    p_create.add_argument("--force", action="store_true", help="기존 인덱스 삭제 후 재생성")

    # finalize
    subparsers.add_parser("finalize", help="적재 완료 후 운영 설정 복원")

    # delete
    subparsers.add_parser("delete", help="인덱스 삭제")

    # status
    subparsers.add_parser("status", help="인덱스 상태 확인")

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
