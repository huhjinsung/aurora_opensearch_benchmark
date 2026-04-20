# 벤치마크 쿼리 상세

본 문서는 Aurora MySQL과 Amazon OpenSearch 성능 비교를 위한 8개 시나리오의 상세 쿼리를 포함합니다.

---

## 시나리오 1: 자계값 비정상 건수 집계

### 목적
자계최대값(`d`)이 비정상 범위(-1500 미만 또는 0 초과)인 전체 건수를 집계합니다. 자계 이상 탐지의 기본 쿼리입니다.

### MySQL 쿼리
```sql
SELECT COUNT(*) 
FROM tgls_param_logs
WHERE CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) < -1500
   OR CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) > 0;
```

### OpenSearch 쿼리
```json
{
  "size": 0,
  "query": {
    "bool": {
      "should": [
        { "range": { "mag_max_val": { "lt": -1500 } } },
        { "range": { "mag_max_val": { "gt": 0 } } }
      ],
      "minimum_should_match": 1
    }
  }
}
```

### 성능 결과
| | MySQL | OpenSearch | 성능 배수 |
|---|---|---|---|
| 응답 시간 | 183,971 ms (~3분 4초) | 31 ms | **6,026x** |
| 결과 건수 | 1,501,021건 | 1,501,021건 | 일치 |

---

## 시나리오 2: 자계 이상 + 위치로그 결합 분석 (고객 핵심 요구)

### 목적
자계최대값이 비정상이면서 위치로그의 자계 측정 점수 평균이 50%(128) 이하인 거래 건수를 집계합니다.

### MySQL 쿼리
```sql
-- 주의: 위치로그 Base64 파싱은 SQL로 불가능
-- 자계값 조건만 적용 가능 (부분 분석)
SELECT COUNT(*)
FROM tgls_param_logs
WHERE (CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) < -1500
    OR CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) > 0)
  AND tgls_prmt_flag_val = 1;  -- 배치에서 미리 계산된 플래그 필요
```

### OpenSearch 쿼리
```json
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
          "bool": {
            "should": [
              { "range": { "mag_max_val": { "lt": -1500 } } },
              { "range": { "mag_max_val": { "gt": 0 } } }
            ]
          }
        },
        { "range": { "loc_mag_avg": { "lte": 128 } } }
      ]
    }
  }
}
```

### 성능 결과
| | MySQL | OpenSearch | 성능 배수 |
|---|---|---|---|
| 응답 시간 | 122,999 ms (~2분 3초) | 106 ms | **1,484.4x** |

### 중요 노트
- **MySQL 한계**: Base64 위치로그를 SQL로 파싱하여 자계 평균을 계산하는 것이 불가능. 별도 배치 처리 필수.
- **OpenSearch**: 적재 시점에 위치로그를 파싱하여 `loc_mag_avg` 필드로 저장, 실시간 쿼리 가능.

---

## 시나리오 3: 역사별/장치별 이상 거래 집계

### 목적
자계값 비정상인 거래를 역사ID × 장치ID별로 건수 집계합니다. 이상 장비 식별 및 교체 우선순위 결정에 활용합니다.

### MySQL 쿼리
```sql
SELECT 
    station_id,
    device_id,
    COUNT(*) as abnormal_count
FROM tgls_param_logs
WHERE CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) < -1500
   OR CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) > 0
GROUP BY station_id, device_id
ORDER BY abnormal_count DESC
LIMIT 100;
```

### OpenSearch 쿼리
```json
{
  "size": 0,
  "query": {
    "bool": {
      "should": [
        { "range": { "mag_max_val": { "lt": -1500 } } },
        { "range": { "mag_max_val": { "gt": 0 } } }
      ],
      "minimum_should_match": 1
    }
  },
  "aggs": {
    "by_station": {
      "terms": { 
        "field": "station_id", 
        "size": 300 
      },
      "aggs": {
        "by_device": {
          "terms": { 
            "field": "device_id", 
            "size": 3000,
            "order": { "_count": "desc" }
          }
        }
      }
    }
  }
}
```

### 성능 결과
| | MySQL | OpenSearch | 성능 배수 |
|---|---|---|---|
| 응답 시간 | 128,252 ms (~2분 8초) | 625 ms | **647.8x** |

---

## 시나리오 4: 시간대별 거래 추이

### 목적
등록일시의 시간(hour)별 거래 건수를 집계합니다. 출퇴근 피크 분석, 시간대별 트래픽 모니터링에 해당합니다.

### MySQL 쿼리
```sql
SELECT 
    SUBSTR(rgt_dtm, 9, 2) as hour,
    COUNT(*) as count
FROM tgls_param_logs
GROUP BY SUBSTR(rgt_dtm, 9, 2)
ORDER BY hour;
```

### OpenSearch 쿼리
```json
{
  "size": 0,
  "aggs": {
    "hourly_transactions": {
      "date_histogram": {
        "field": "rgt_dtm",
        "calendar_interval": "hour",
        "format": "yyyy-MM-dd HH:00"
      }
    }
  }
}
```

### 성능 결과
| | MySQL | OpenSearch | 성능 배수 |
|---|---|---|---|
| 응답 시간 | 9,232 ms (~9.2초) | 243 ms | **57.0x** |

---

## 시나리오 5: zone별 IN/OUT 통계

### 목적
입장(I)/출장(O) zone별 거래 건수 분포를 집계합니다.

### MySQL 쿼리
```sql
SELECT 
    JSON_UNQUOTE(JSON_EXTRACT(tgls_prmt_log_val, '$.b')) as initial_zone,
    COUNT(*) as count
FROM tgls_param_logs
GROUP BY initial_zone;
```

### OpenSearch 쿼리
```json
{
  "size": 0,
  "aggs": {
    "zone_distribution": {
      "terms": { 
        "field": "initial_zone",
        "size": 10
      }
    }
  }
}
```

### 성능 결과
| | MySQL | OpenSearch | 성능 배수 |
|---|---|---|---|
| 응답 시간 | 74,421 ms (~1분 14초) | 74 ms | **2,304.2x** |

---

## 시나리오 6: 장치 상태별 이상률

### 목적
장치 상태(`u`)별 전체 건수 및 자계 이상 건수를 집계합니다. 장치 상태와 이상 발생의 상관관계 분석에 활용합니다.

### MySQL 쿼리
```sql
SELECT 
    JSON_UNQUOTE(JSON_EXTRACT(tgls_prmt_log_val, '$.u')) as device_status,
    COUNT(*) as total_count,
    SUM(CASE 
        WHEN CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) < -1500 
          OR CAST(JSON_EXTRACT(tgls_prmt_log_val, '$.d') AS SIGNED) > 0 
        THEN 1 
        ELSE 0 
    END) as abnormal_count
FROM tgls_param_logs
GROUP BY device_status;
```

### OpenSearch 쿼리
```json
{
  "size": 0,
  "aggs": {
    "by_device_status": {
      "terms": { 
        "field": "device_status",
        "size": 20
      },
      "aggs": {
        "abnormal_count": {
          "filter": {
            "bool": {
              "should": [
                { "range": { "mag_max_val": { "lt": -1500 } } },
                { "range": { "mag_max_val": { "gt": 0 } } }
              ]
            }
          }
        }
      }
    }
  }
}
```

### 성능 결과
| | MySQL | OpenSearch | 성능 배수 |
|---|---|---|---|
| 응답 시간 | 203,693 ms (~3분 24초) | 1,439 ms | **356.7x** |

---

## 시나리오 7: 특정 역사 거래 이력 조회

### 목적
특정 역사(ST-001)의 최근 100건 거래를 시간 역순으로 조회합니다.

### MySQL 쿼리
```sql
SELECT *
FROM tgls_param_logs
WHERE station_id = 'ST-001'
ORDER BY rgt_dtm DESC
LIMIT 100;
```

### OpenSearch 쿼리
```json
{
  "size": 100,
  "query": {
    "term": { 
      "station_id": "ST-001" 
    }
  },
  "sort": [
    { "rgt_dtm": { "order": "desc" } }
  ]
}
```

### 성능 결과
| | MySQL | OpenSearch | 성능 배수 |
|---|---|---|---|
| 응답 시간 | 110 ms | 134 ms | **1.3x** |

### 중요 노트
- **MySQL 우위**: B-Tree 인덱스가 있는 단건 조회에서 MySQL이 동등하거나 더 빠른 성능을 보임.
- 조기 종료(early termination) 활용으로 효율적인 처리 가능.

---

## 시나리오 8: 역사명 전문 검색

### 목적
'강남' 키워드가 포함된 역사의 거래를 검색합니다.

### MySQL 쿼리
```sql
SELECT *
FROM tgls_param_logs
WHERE tgls_prmt_log_val LIKE '%강남%'
LIMIT 50;
```

### OpenSearch 쿼리
```json
{
  "size": 50,
  "query": {
    "match": {
      "station_name": {
        "query": "강남",
        "analyzer": "seunjeon"
      }
    }
  }
}
```

### 성능 결과
| | MySQL | OpenSearch | 성능 배수 |
|---|---|---|---|
| 응답 시간 | 24,481 ms (~24.5초) | 141 ms | **507.6x** |

### 중요 노트
- **MySQL**: `LIKE '%강남%'`로 VARCHAR 전체를 스캔 (전수 스캔)
- **OpenSearch**: seunjeon 한글 형태소 분석기로 토크나이징된 역인덱스 활용
