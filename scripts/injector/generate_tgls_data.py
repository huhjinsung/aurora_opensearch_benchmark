#!/usr/bin/env python3
"""
태그리스 파라미터로그 테스트 데이터 생성기

실제 티머니 태그리스 데이터 구조를 재현합니다.
- 파라미터로그 JSON (키 a~u)
- 위치로그 Base64 (바이트 시계열: IN BLE, OUT BLE, 자계 점수)
- 이벤트로그

JSON Lines (.jsonl) 형식으로 출력합니다.

사용법:
  python3 generate_tgls_data.py --count 10000000 --output tgls_10m.jsonl
"""

import argparse
import base64
import json
import random
import struct
import time
from datetime import datetime, timedelta


# =============================================================================
# 역사 마스터 데이터 (서울 지하철 300개 역사)
# =============================================================================
STATION_NAMES = [
    "강남역", "역삼역", "선릉역", "삼성역", "종합운동장역", "잠실새내역", "잠실역",
    "석촌역", "송파역", "가락시장역", "수서역", "대치역", "도곡역", "양재역",
    "매봉역", "서울역", "시청역", "종각역", "종로3가역", "종로5가역", "동대문역",
    "동대문역사문화공원역", "신당역", "상왕십리역", "왕십리역", "한양대역",
    "뚝섬역", "성수역", "건대입구역", "구의역", "강변역", "잠실나루역",
    "홍대입구역", "합정역", "당산역", "영등포구청역", "문래역", "신도림역",
    "구로역", "구로디지털단지역", "가산디지털단지역", "독산역", "금천구청역",
    "사당역", "이수역", "낙성대역", "서울대입구역", "봉천역", "신림역",
    "교대역", "방배역", "서초역", "고속터미널역", "신논현역", "논현역",
    "학동역", "강남구청역", "청담역", "압구정로데오역", "압구정역",
    "신사역", "잠원역", "반포역", "남부터미널역", "내방역",
    "여의도역", "마포역", "공덕역", "애오개역", "충정로역", "서대문역",
    "광화문역", "안국역", "혜화역", "한성대입구역", "성신여대입구역",
    "동묘앞역", "창신역", "보문역", "신설동역", "제기동역", "청량리역",
    "회기역", "외대앞역", "신이문역", "석계역", "돌곶이역",
    "명동역", "회현역", "남산역", "이태원역", "녹사평역", "삼각지역",
    "숙대입구역", "남영역", "용산역", "이촌역", "서빙고역",
    "한남역", "옥수역", "금호역", "약수역", "버티고개역", "동작역",
    "총신대입구역", "남태령역", "선바위역", "경마공원역", "대공원역",
    "과천역", "정부과천청사역", "인덕원역", "평촌역", "범계역",
    "금정역", "산본역", "수리산역", "대야미역", "반월역", "상록수역",
    "한대앞역", "중앙역", "고잔역", "초지역", "안산역", "신길온천역",
    "오이도역", "정왕역", "월곶역", "소래포구역", "인천역",
    "노량진역", "대방역", "신풍역", "보라매역", "신대방삼거리역",
    "장승배기역", "상도역", "숭실대입구역", "신대방역",
    "천호역", "강동역", "길동역", "굽은다리역", "명일역",
    "고덕역", "상일동역", "둔촌동역", "올림픽공원역",
    "방이역", "오금역", "개롱역", "거여역", "마천역",
    "개화산역", "김포공항역", "송정역", "마곡역", "마곡나루역",
    "발산역", "우장산역", "화곡역", "까치산역", "신정역",
    "목동역", "오목교역", "양평역", "선유도역",
    "미아역", "미아사거리역", "길음역", "돈암역", "월곡역",
    "상월곡역", "하월곡역", "쌍문역", "수유역", "도봉산역",
    "방학역", "창동역", "노원역", "상계역", "당고개역", "중계역",
    "마들역", "태릉입구역", "화랑대역", "봉화산역", "먹골역",
    "중화역", "상봉역", "면목역", "사가정역",
    "용마산역", "중곡역", "군자역", "아차산역", "광나루역",
    "천왕역", "광명사거리역", "온수역", "개봉역", "오류동역",
    "역곡역", "소사역", "부천역", "중동역", "송내역", "부개역",
    "인천시청역", "간석역", "주안역", "도화역", "제물포역",
    "도원역", "동인천역", "인천터미널역", "예술회관역",
    "센트럴파크역", "국제업무지구역", "테크노파크역",
    "신촌역", "이대역", "아현역", "대흥역", "광흥창역", "상수역",
    "망원역", "월드컵경기장역", "디지털미디어시티역", "수색역",
    "화전역", "행신역", "능곡역", "대곡역", "백석역",
    "풍산역", "일산역", "탄현역", "야당역", "운정역",
    "금릉역", "파주역", "문산역", "장항역",
    "판교역", "이매역", "야탑역", "서현역", "수내역",
    "정자역", "미금역", "오리역", "수원역", "영통역",
    "망포역", "매탄권선역", "수원시청역", "매교역",
    "고색역", "오목천역", "어천역", "동탄역",
    "병점역", "세마역", "서동탄역", "봉담역",
    "광교역", "광교중앙역", "상현역", "성복역",
    "수지구청역", "동천역", "미금역", "보정역",
    "죽전역", "구성역", "신갈역", "기흥역", "상갈역",
    "청명역", "영통역", "망포역", "매탄권선역",
    "안양역", "관악역", "석수역", "금천구청역",
    "시흥역", "시흥능곡역", "달월역", "월곶역",
    "소래포구역", "인천논현역", "호구포역", "남동인더스파크역",
    "원인재역", "연수역", "동막역", "동춘역",
    "캠퍼스타운역", "테크노파크역",
]


def build_stations(n=300):
    """역사 마스터 데이터 생성 (300개)"""
    stations = []
    names = STATION_NAMES[:n] if len(STATION_NAMES) >= n else STATION_NAMES * (n // len(STATION_NAMES) + 1)
    for i in range(n):
        stations.append({
            "station_id": f"ST-{i+1:03d}",
            "station_name": names[i],
        })
    return stations


def build_devices(stations, per_station=10):
    """장치 마스터 데이터 생성 (역사당 10대)"""
    devices = []
    seq = 1
    for st in stations:
        n_devices = random.randint(per_station - 3, per_station + 3)
        for _ in range(n_devices):
            devices.append({
                "device_id": f"DEV-{seq:05d}",
                "station_id": st["station_id"],
                "station_name": st["station_name"],
            })
            seq += 1
    return devices


# =============================================================================
# 파라미터로그 JSON 생성 (키 a~u)
# =============================================================================
DEVICE_STATUSES = [
    "NORMAL/OFF/FOREGROUND_SERVICE",
    "NORMAL/OFF/FOREGROUND",
    "NORMAL/ON/FOREGROUND_SERVICE",
    "NORMAL/ON/FOREGROUND",
    "NORMAL/OFF/BACKGROUND",
]

TRANSACTION_TYPES = ["1", "2", "3"]


def gen_param_log(is_abnormal_mag=False):
    """태그리스 파라미터로그 JSON dict 생성 (키 a~u)"""
    base_ts = random.randint(100000000, 200000000)

    zone = random.choice(["I", "O"])
    zone_code = "1001" if zone == "I" else "1000"

    # 자계최대값: 정상(-1500~0), 비정상(그 외)
    if is_abnormal_mag:
        mag_max = random.choice([
            random.randint(-3000, -1501),
            random.randint(1, 500),
        ])
    else:
        mag_max = random.randint(-1500, 0)

    mag_secondary = random.randint(-2000, 500)

    def ble_pair():
        return f"{random.randint(0, 5000)},{random.randint(0, 5000)}"

    def ble_ts():
        return str(base_ts + random.randint(1000, 10000))

    log = {
        "a": str(base_ts),
        "b": zone,
        "c": zone_code,
        "d": str(mag_max),
        "e": str(base_ts + random.randint(1000, 5000)),
        "f": ble_pair(),
        "g": ble_ts(),
        "h": ble_pair(),
        "i": ble_ts(),
        "j": ble_pair(),
        "k": ble_ts(),
        "l": str(mag_secondary),
        "m": ble_pair(),
        "n": ble_ts(),
        "o": ble_pair() if random.random() > 0.3 else "",
        "p": ble_pair() if random.random() > 0.5 else "",
        "t": random.choice(TRANSACTION_TYPES),
        "u": random.choice(DEVICE_STATUSES),
    }
    return log, mag_max


# =============================================================================
# 위치로그 Base64 생성
# =============================================================================
def gen_location_log(is_abnormal_loc=False):
    """
    위치로그 바이트 시계열 생성 → Base64 인코딩

    매 3바이트: [IN BLE 점수(1B), OUT BLE 점수(1B), 자계 점수(1B)]
    자계 점수: 0~255 (정상 128 이상, 비정상 128 미만)
    측정 시점: 20~40개
    """
    n_samples = random.randint(20, 40)
    raw_bytes = bytearray()
    mag_scores = []

    for _ in range(n_samples):
        ble_in = random.randint(0, 255)
        ble_out = random.randint(0, 255)

        if is_abnormal_loc:
            mag_score = random.randint(0, 127)
        else:
            mag_score = random.randint(80, 255)

        raw_bytes.extend([ble_in, ble_out, mag_score])
        mag_scores.append(mag_score)

    loc_base64 = base64.b64encode(bytes(raw_bytes)).decode("ascii")
    mag_avg = sum(mag_scores) / len(mag_scores) if mag_scores else 0

    return loc_base64, mag_scores, mag_avg


# =============================================================================
# 이벤트로그 생성 (간단한 인코딩 시뮬레이션)
# =============================================================================
def gen_event_log():
    """이벤트로그 Base64 (임의 바이트열)"""
    n_bytes = random.randint(30, 80)
    raw = bytes([random.randint(0, 255) for _ in range(n_bytes)])
    return base64.b64encode(raw).decode("ascii")


# =============================================================================
# 시간대별 가중치
# =============================================================================
HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2,
    5, 15, 25, 20, 10, 8,
    8, 8, 8, 10, 12, 20,
    25, 15, 8, 5, 3, 2,
]


# =============================================================================
# Main
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="태그리스 파라미터로그 데이터 생성")
    parser.add_argument("--count", type=int, required=True, help="생성할 총 건수")
    parser.add_argument("--output", required=True, help="출력 파일 (.jsonl)")
    parser.add_argument("--days", type=int, default=30, help="데이터 기간 (일)")
    parser.add_argument("--seed", type=int, default=42, help="랜덤 시드")
    parser.add_argument("--abnormal-rate", type=float, default=0.15,
                        help="자계 이상 거래 비율 (default: 0.15)")
    args = parser.parse_args()

    random.seed(args.seed)

    stations = build_stations(300)
    devices = build_devices(stations, per_station=10)
    print(f"[INFO] 역사 {len(stations)}개, 장치 {len(devices)}대 생성", flush=True)

    end_date = datetime(2026, 4, 16)
    start_date = end_date - timedelta(days=args.days)
    date_range = [start_date + timedelta(days=d) for d in range(args.days)]

    print(f"[INFO] 데이터 생성 시작", flush=True)
    print(f"  - 건수: {args.count:,}", flush=True)
    print(f"  - 기간: {start_date.date()} ~ {end_date.date()} ({args.days}일)", flush=True)
    print(f"  - 이상 거래 비율: {args.abnormal_rate:.0%}", flush=True)
    print(f"  - 출력: {args.output}", flush=True)

    t0 = time.time()
    txn_seq = 0

    with open(args.output, "w", encoding="utf-8") as f:
        for i in range(args.count):
            device = random.choice(devices)
            base_date = random.choice(date_range)

            hour = random.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            dt = base_date.replace(hour=hour, minute=minute, second=second)
            rgt_dtm = dt.strftime("%Y%m%d%H%M%S")

            # 이상 여부 결정
            is_abnormal = random.random() < args.abnormal_rate
            # 이상 거래 중 일부는 자계+위치 모두 이상, 일부는 자계만 이상
            is_abnormal_mag = is_abnormal
            is_abnormal_loc = is_abnormal and random.random() < 0.6

            # 파라미터로그 JSON
            param_log, mag_max_val = gen_param_log(is_abnormal_mag)

            # 위치로그 Base64
            loc_log, loc_mag_scores, loc_mag_avg = gen_location_log(is_abnormal_loc)

            # 이벤트로그
            event_log = gen_event_log()

            # 트랜잭션 ID
            txn_seq += 1
            txn_id = f"TXN-{rgt_dtm[:8]}-{txn_seq:08d}"

            record = {
                "station_id": device["station_id"],
                "station_name": device["station_name"],
                "device_id": device["device_id"],
                "rgt_dtm": rgt_dtm,
                "transaction_id": txn_id,
                "param_log": param_log,
                "loc_log": loc_log,
                "event_log": event_log,
                "loc_mag_avg": round(loc_mag_avg, 2),
                "loc_mag_scores": loc_mag_scores,
                "mag_max_val": mag_max_val,
                "flag_val": None,
            }

            f.write(json.dumps(record, ensure_ascii=False) + "\n")

            if (i + 1) % 1_000_000 == 0:
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed
                print(f"  [{i+1:>12,} / {args.count:,}] {elapsed:.1f}s ({rate:,.0f} rows/s)", flush=True)

    elapsed = time.time() - t0
    print(f"\n[OK] 완료: {args.count:,}건, {elapsed:.1f}초 ({args.count/elapsed:,.0f} rows/s)", flush=True)
    print(f"  파일: {args.output}", flush=True)


if __name__ == "__main__":
    main()
