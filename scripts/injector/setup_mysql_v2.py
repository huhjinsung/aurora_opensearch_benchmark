#!/usr/bin/env python3
"""
Aurora MySQL 테이블 생성 (v2 - 태그리스 파라미터로그)

고객 현행 구조(tbdtvh114)를 재현합니다.
station_id, device_id에는 MySQL에 유리하도록 인덱스를 추가했습니다.

사용법:
  python3 setup_mysql_v2.py --host AURORA_ENDPOINT --user admin --password PASSWORD
"""

import argparse
import pymysql


DDL = """
CREATE TABLE IF NOT EXISTS tgls_param_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    tgls_prmt_log_val VARCHAR(4000) COMMENT '태그리스 파라미터로그 JSON',
    tgls_loc_log_larg_ctt VARCHAR(3000) COMMENT '위치로그 Base64',
    tgls_evnt_log_larg_ctt VARCHAR(3000) COMMENT '이벤트로그',
    tgls_prmt_flag_val INT DEFAULT NULL COMMENT '배치 분석 플래그',
    station_id VARCHAR(10) COMMENT '역사 ID',
    device_id VARCHAR(20) COMMENT '장치 ID',
    rgsr_id VARCHAR(20) DEFAULT 'SYSTEM' COMMENT '등록자 ID',
    rgt_dtm VARCHAR(14) COMMENT '등록일시 (yyyyMMddHHmmss)',
    moapp_trns_trd_trnc_id VARCHAR(70) COMMENT '모바일앱 트랜잭션ID',
    INDEX idx_rgt_dtm (rgt_dtm),
    INDEX idx_station_id (station_id),
    INDEX idx_device_id (device_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def main():
    parser = argparse.ArgumentParser(description="Aurora MySQL 테이블 생성 (v2)")
    parser.add_argument("--host", required=True, help="Aurora writer endpoint")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="poc_tmoney")
    parser.add_argument("--drop", action="store_true", help="기존 테이블 삭제 후 재생성")
    args = parser.parse_args()

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {args.database}")
            cursor.execute(f"USE {args.database}")
            print(f"[OK] 데이터베이스 '{args.database}' 준비 완료", flush=True)

            if args.drop:
                cursor.execute("DROP TABLE IF EXISTS tgls_param_logs")
                print("[OK] 기존 tgls_param_logs 테이블 삭제", flush=True)

            cursor.execute(DDL)
            conn.commit()
            print("[OK] 테이블 'tgls_param_logs' 생성 완료", flush=True)
            print("     - tgls_prmt_log_val: VARCHAR(4000) (JSON 파라미터로그)", flush=True)
            print("     - tgls_loc_log_larg_ctt: VARCHAR(3000) (위치로그 Base64)", flush=True)
            print("     - tgls_evnt_log_larg_ctt: VARCHAR(3000) (이벤트로그)", flush=True)
            print("     - station_id: VARCHAR(10) + INDEX", flush=True)
            print("     - device_id: VARCHAR(20) + INDEX", flush=True)
            print("     - rgt_dtm: VARCHAR(14) + INDEX", flush=True)

            cursor.execute("SHOW TABLE STATUS LIKE 'tgls_param_logs'")
            row = cursor.fetchone()
            print(f"\n===== 테이블 상태 =====", flush=True)
            print(f"  Engine:  {row[1]}", flush=True)
            print(f"  Rows:    {row[4]}", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
