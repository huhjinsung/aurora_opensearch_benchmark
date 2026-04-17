#!/usr/bin/env python3
"""
OpenSearch 데이터 적재 스크립트

generate_data.py로 생성한 .jsonl 파일을 읽어서 Bulk API로 적재합니다.

사전 조건:
  - setup_opensearch.py create 로 인덱스 생성 완료 (replica=0, refresh=-1)

사용법:
  python3 load_opensearch.py \
    --host OPENSEARCH_ENDPOINT \
    --user admin --password PASSWORD \
    --input data_10m.jsonl

적재 완료 후:
  python3 setup_opensearch.py finalize --host ENDPOINT --user admin --password PASSWORD
"""

import argparse
import json
import time
from opensearchpy import OpenSearch, helpers


INDEX_NAME = "device-event-logs"
BULK_SIZE = 2000  # Bulk API 배치 크기 (메모리 절약)


def doc_generator(filepath, index_name):
    """jsonl 파일을 읽어서 Bulk API 형식으로 변환하는 제너레이터"""
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            yield {
                "_index": index_name,
                "_source": doc,
            }


def main():
    parser = argparse.ArgumentParser(description="OpenSearch 데이터 적재")
    parser.add_argument("--host", required=True, help="OpenSearch endpoint")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", required=True)
    parser.add_argument("--input", required=True, help=".jsonl 파일 경로")
    parser.add_argument("--bulk-size", type=int, default=BULK_SIZE, help="Bulk 배치 크기")
    args = parser.parse_args()

    client = OpenSearch(
        hosts=[{"host": args.host, "port": 443}],
        http_auth=(args.user, args.password),
        use_ssl=True,
        verify_certs=True,
        ssl_show_warn=False,
    )

    # 인덱스 존재 확인
    if not client.indices.exists(index=INDEX_NAME):
        print(f"[ERROR] 인덱스 '{INDEX_NAME}'가 존재하지 않습니다.")
        print(f"  먼저 실행: python3 setup_opensearch.py create --host {args.host} ...")
        return

    print(f"[INFO] OpenSearch 적재 시작")
    print(f"  - host: {args.host}")
    print(f"  - index: {INDEX_NAME}")
    print(f"  - input: {args.input}")
    print(f"  - bulk_size: {args.bulk_size}")

    t0 = time.time()
    total = 0
    errors = 0

    for ok, result in helpers.parallel_bulk(
        client,
        doc_generator(args.input, INDEX_NAME),
        chunk_size=args.bulk_size,
        thread_count=2,
        raise_on_error=False,
    ):
        if ok:
            total += 1
        else:
            errors += 1
            if errors <= 5:
                print(f"  [ERROR] {result}")

        if total > 0 and total % 100_000 == 0:
            elapsed = time.time() - t0
            rate = total / elapsed
            print(f"  [{total:>12,}] {elapsed:.1f}s ({rate:,.0f} docs/s)", flush=True)

    elapsed = time.time() - t0
    print(f"\n[OK] OpenSearch 적재 완료")
    print(f"  - 성공: {total:,}")
    print(f"  - 실패: {errors:,}")
    print(f"  - 소요 시간: {elapsed:.1f}초")
    print(f"  - 처리 속도: {total/elapsed:,.0f} docs/s")
    print()
    print(f"[NEXT] 운영 설정 복원:")
    print(f"  python3 setup_opensearch.py finalize --host {args.host} --user {args.user} --password ****")


if __name__ == "__main__":
    main()
