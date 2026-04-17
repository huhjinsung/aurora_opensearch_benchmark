#!/usr/bin/env python3
"""
장비 이벤트 로그 데이터 생성기

출퇴근 피크 시간대 반영, 장비 1,000대, 8종 이벤트 시뮬레이션.
JSON Lines (.jsonl) 형식으로 출력합니다.

사용법:
  # 100만 건 생성 (검증용)
  python3 generate_data.py --count 1000000 --output data_1m.jsonl

  # 1,000만 건 생성
  python3 generate_data.py --count 10000000 --output data_10m.jsonl

  # 5,000만 건 생성
  python3 generate_data.py --count 50000000 --output data_50m.jsonl
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta

# =============================================================================
# 상수 정의
# =============================================================================

# 장비 1,000대
DEVICE_TYPES = {
    "bus_reader":    500,  # 버스 리더기 500대
    "subway_gate":   300,  # 지하철 게이트 300대
    "charger":       100,  # 충전기 100대
    "kiosk":         100,  # 키오스크 100대
}

EVENT_TYPE_WEIGHTS = {
    "tap_on":           35,
    "tap_off":          30,
    "transfer":         10,
    "charge":            5,
    "heartbeat":        10,
    "error":             5,
    "boot":              3,
    "firmware_update":   2,
}

# 장비별 가능한 이벤트
DEVICE_EVENTS = {
    "bus_reader":   ["tap_on", "tap_off", "transfer", "heartbeat", "error", "boot", "firmware_update"],
    "subway_gate":  ["tap_on", "tap_off", "transfer", "heartbeat", "error", "boot", "firmware_update"],
    "charger":      ["charge", "heartbeat", "error", "boot", "firmware_update"],
    "kiosk":        ["charge", "heartbeat", "error", "boot", "firmware_update"],
}

LOCATIONS = [
    "강남역 정류장", "서울역 1번출구", "잠실역 3번출구", "홍대입구역",
    "신촌역", "명동역 2번출구", "여의도역", "종로3가역",
    "삼성역 코엑스", "교대역", "사당역", "건대입구역",
    "왕십리역", "합정역", "이태원역", "용산역",
    "구로디지털단지역", "가산디지털단지역", "판교역", "수원역",
    "인천터미널", "부천역", "일산역", "분당서현역",
    "동탄역", "광명역", "천안아산역", "대전역",
    "세종시 BRT정류장", "김포공항역",
]

ROUTE_IDS = [f"RT-{i:03d}" for i in range(1, 201)]  # 200개 노선

REGION_CODES = ["SEL", "ICN", "GGN", "GGS", "DJN", "CHN", "BSN"]

FIRMWARE_VERSIONS = ["3.0.0", "3.1.0", "3.1.5", "3.2.0", "3.2.1", "3.3.0"]

ERROR_CODES = ["E001", "E002", "E003", "E010", "E020", "E030", "E099"]

PAYMENT_STATUSES = ["success", "success", "success", "success", "success",
                    "success", "success", "success", "success", "fail"]  # 10% 실패

# 시간대별 가중치 (출퇴근 피크 반영)
HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2,     # 00-05: 심야
    5, 15, 25, 20, 10, 8,  # 06-11: 출근 피크(07-09)
    8, 8, 8, 10, 12, 20,   # 12-17: 오후
    25, 15, 8, 5, 3, 2,    # 18-23: 퇴근 피크(18-19)
]


# =============================================================================
# 장비 마스터 데이터 생성
# =============================================================================
def build_devices():
    """장비 1,000대의 마스터 데이터 생성"""
    devices = []
    seq = 1
    for dtype, count in DEVICE_TYPES.items():
        prefix = {
            "bus_reader": "TM-BUS",
            "subway_gate": "TM-SUB",
            "charger": "TM-CHG",
            "kiosk": "TM-KSK",
        }[dtype]

        for _ in range(count):
            devices.append({
                "device_id": f"{prefix}-{seq:05d}",
                "device_type": dtype,
                "location": random.choice(LOCATIONS),
                "region_code": random.choice(REGION_CODES),
                "firmware_ver": random.choice(FIRMWARE_VERSIONS),
                "route_id": random.choice(ROUTE_IDS) if dtype in ("bus_reader", "subway_gate") else None,
            })
            seq += 1
    return devices


# =============================================================================
# 이벤트 생성
# =============================================================================
def pick_hour():
    """시간대별 가중치에 따라 시(hour) 선택"""
    return random.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]


def generate_event(device, base_date):
    """단일 이벤트 JSON dict 생성"""
    # 장비가 발생시킬 수 있는 이벤트 중 가중치 기반 선택
    possible = DEVICE_EVENTS[device["device_type"]]
    weights = [EVENT_TYPE_WEIGHTS[e] for e in possible]
    event_type = random.choices(possible, weights=weights, k=1)[0]

    # 시간 생성
    hour = pick_hour()
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    event_time = base_date.replace(hour=hour, minute=minute, second=second)

    event = {
        "device_id": device["device_id"],
        "device_type": device["device_type"],
        "event_type": event_type,
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "location": device["location"],
        "region_code": device["region_code"],
        "firmware_ver": device["firmware_ver"],
        "battery_level": random.randint(5, 100),
        "status": random.choice(PAYMENT_STATUSES),
        "created_at": event_time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    # 이벤트 유형별 추가 필드
    if event_type in ("tap_on", "tap_off", "transfer"):
        event["route_id"] = device["route_id"] or random.choice(ROUTE_IDS)
        event["card_number"] = f"{random.randint(1000,9999)}-****-{random.randint(1000,9999)}"
        event["fare_amount"] = random.choice([1250, 1500, 1800, 2000, 2500])
        event["error_code"] = None
    elif event_type == "charge":
        event["route_id"] = None
        event["card_number"] = f"{random.randint(1000,9999)}-****-{random.randint(1000,9999)}"
        event["fare_amount"] = random.choice([5000, 10000, 20000, 50000])
        event["error_code"] = None
    elif event_type == "error":
        event["route_id"] = device.get("route_id")
        event["card_number"] = None
        event["fare_amount"] = None
        event["error_code"] = random.choice(ERROR_CODES)
    else:  # heartbeat, boot, firmware_update
        event["route_id"] = device.get("route_id")
        event["card_number"] = None
        event["fare_amount"] = None
        event["error_code"] = None

    return event


# =============================================================================
# Main
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="장비 이벤트 로그 데이터 생성")
    parser.add_argument("--count", type=int, required=True, help="생성할 총 건수")
    parser.add_argument("--output", required=True, help="출력 파일 (.jsonl)")
    parser.add_argument("--days", type=int, default=30, help="데이터 기간 (일, default: 30)")
    parser.add_argument("--seed", type=int, default=42, help="랜덤 시드")
    args = parser.parse_args()

    random.seed(args.seed)

    devices = build_devices()
    end_date = datetime(2026, 4, 16)
    start_date = end_date - timedelta(days=args.days)
    date_range = [start_date + timedelta(days=d) for d in range(args.days)]

    print(f"[INFO] 데이터 생성 시작")
    print(f"  - 건수: {args.count:,}")
    print(f"  - 기간: {start_date.date()} ~ {end_date.date()} ({args.days}일)")
    print(f"  - 장비: {len(devices):,}대")
    print(f"  - 출력: {args.output}")

    t0 = time.time()
    with open(args.output, "w", encoding="utf-8") as f:
        for i in range(args.count):
            device = random.choice(devices)
            base_date = random.choice(date_range)
            event = generate_event(device, base_date)
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

            if (i + 1) % 1_000_000 == 0:
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed
                print(f"  [{i+1:>12,} / {args.count:,}] {elapsed:.1f}s ({rate:,.0f} rows/s)")

    elapsed = time.time() - t0
    print(f"\n[OK] 완료: {args.count:,}건, {elapsed:.1f}초 ({args.count/elapsed:,.0f} rows/s)")
    print(f"  파일: {args.output}")


if __name__ == "__main__":
    main()
