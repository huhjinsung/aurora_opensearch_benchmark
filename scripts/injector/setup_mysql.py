#!/usr/bin/env python3
"""
Aurora MySQL 테이블 생성 스크립트

고객 현행 방식(TEXT 컬럼에 JSON 통째로 저장)을 재현합니다.

사용법:
  python3 setup_mysql.py --host AURORA_ENDPOINT --user admin --password PASSWORD
"""

import argparse
import pymysql


DDL = """
CREATE TABLE IF NOT EXISTS device_event_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    log_data TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def main():
    parser = argparse.ArgumentParser(description="Aurora MySQL 테이블 생성")
    parser.add_argument("--host", required=True, help="Aurora writer endpoint")
    parser.add_argument("--port", type=int, default=3306, help="Port (default: 3306)")
    parser.add_argument("--user", default="admin", help="Master username")
    parser.add_argument("--password", required=True, help="Master password")
    parser.add_argument("--database", default="poc_tmoney", help="Database name")
    args = parser.parse_args()

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
    )

    try:
        with conn.cursor() as cursor:
            # 데이터베이스 생성
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {args.database}")
            cursor.execute(f"USE {args.database}")
            print(f"[OK] 데이터베이스 '{args.database}' 준비 완료")

            # 테이블 생성
            cursor.execute(DDL)
            conn.commit()
            print(f"[OK] 테이블 'device_event_logs' 생성 완료")
            print(f"     - id: BIGINT AUTO_INCREMENT PK")
            print(f"     - log_data: TEXT (JSON 통째로 저장)")
            print(f"     - created_at: DATETIME + INDEX")

            # 확인
            cursor.execute("SHOW TABLE STATUS LIKE 'device_event_logs'")
            row = cursor.fetchone()
            print(f"\n===== 테이블 상태 =====")
            print(f"  Engine:  {row[1]}")
            print(f"  Rows:    {row[4]}")
            print(f"  Charset: utf8mb4")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
