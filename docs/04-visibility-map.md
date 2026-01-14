# Visibility Map과 Index-Only Scan

## 개요

Visibility Map은 MVCC의 가시성 검사를 최적화하는 핵심 메커니즘입니다.
Lab 02b의 Snapshot 개념과 직접 연결됩니다.

## 핵심 연결: Snapshot → Visibility Map

### 복습: Snapshot이란? (Lab 02b)

```
Snapshot = (xmin, xmax, xip[])

- xmin: 스냅샷 생성 시점의 가장 오래된 활성 트랜잭션 ID
- xmax: 스냅샷 생성 시점의 다음 할당될 트랜잭션 ID
- xip[]: 스냅샷 생성 시점에 진행 중인 트랜잭션 ID 목록
```

### 문제: 모든 행에서 가시성 검사?

인덱스 스캔으로 1000개 행을 찾았다고 가정:
1. 각 행의 ctid로 heap 접근
2. xmin, xmax 읽기
3. Snapshot과 비교하여 가시성 판단
4. 1000번 반복!

→ 비효율적! 특히 대부분의 행이 "당연히 보이는" 경우

### 해결: Visibility Map

```
┌─────────────────────────────────────────────┐
│ Visibility Map                              │
├─────────────────────────────────────────────┤
│ Page 0: [1]  ← all-visible                 │
│ Page 1: [0]  ← NOT all-visible (최근 변경) │
│ Page 2: [1]  ← all-visible                 │
│ Page 3: [1]  ← all-visible                 │
│ ...                                         │
└─────────────────────────────────────────────┘

all-visible = 이 페이지의 모든 tuple이
              "모든 트랜잭션에 visible"
```

## Visibility Map의 동작

### all-visible 플래그 설정

```
1. VACUUM이 페이지의 dead tuple 정리
2. 페이지 내 모든 tuple이 committed 상태 확인
3. 해당 페이지를 all-visible로 표시

all-visible 해제 조건:
- 페이지 내 행이 UPDATE/DELETE됨
- 새로운 uncommitted 행이 INSERT됨
```

### Index-Only Scan 동작

```
┌─────────────────────────────────────────────────────────────────┐
│ Index-Only Scan 조건                                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 1. 필요한 모든 컬럼이 인덱스에 있음 (Covering Index)            │
│                                                                  │
│    CREATE INDEX idx ON orders(customer_id)                       │
│      INCLUDE (total_amount, status);  ← 페이로드                 │
│                                                                  │
│ 2. Visibility Map에서 해당 페이지가 all-visible                 │
│                                                                  │
│    → 두 조건 만족 시 heap 접근 불필요!                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 실행 계획에서 확인

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT customer_id, total_amount
FROM orders
WHERE customer_id BETWEEN 100 AND 200;

결과:
Index Only Scan using idx_orders_covering on orders
  Index Cond: ...
  Heap Fetches: 0  ← 모든 페이지가 all-visible!
```

**Heap Fetches** 해석:
- `Heap Fetches: 0` → 완벽한 Index-Only Scan
- `Heap Fetches: N` → N개 행에서 heap 확인 필요 (all-visible이 아님)

## Snapshot과 Visibility Map의 관계

```
일반 Index Scan:
  인덱스 → ctid → heap → xmin/xmax 확인 → Snapshot 비교

Index-Only Scan (all-visible):
  인덱스 → 값 반환  (heap 접근 생략!)

★ all-visible = "Snapshot 확인 없이 바로 읽어도 됨"
  = 모든 트랜잭션에 동일하게 보이는 데이터
```

## VACUUM의 역할

```
VACUUM이 Visibility Map에 미치는 영향:

1. Dead tuple 정리
2. 페이지 상태 확인
3. 모든 tuple이 committed → all-visible 표시
4. 추가로 freeze → all-frozen 표시

all-frozen:
  - 모든 tuple의 xmin이 frozen됨
  - 트랜잭션 ID wraparound 방지
```

## 실무 최적화 가이드

### Covering Index 설계

```sql
-- 자주 사용되는 쿼리 분석
SELECT customer_id, total_amount, status
FROM orders
WHERE customer_id = ?;

-- Covering Index 생성
CREATE INDEX idx_orders_covering
ON orders(customer_id)
INCLUDE (total_amount, status);

-- 검색 키: customer_id (WHERE, ORDER BY)
-- 페이로드: total_amount, status (SELECT만)
```

### Index-Only Scan 유도 방법

1. **Covering Index 생성**: 필요한 컬럼 모두 포함
2. **정기적 VACUUM**: all-visible 페이지 유지
3. **쓰기 패턴 고려**: 자주 UPDATE되는 테이블은 효과 감소

### Heap Fetches가 높을 때

```
원인:
1. 최근 UPDATE/DELETE가 많음
2. VACUUM이 아직 실행 안됨
3. 활발히 변경되는 테이블

해결:
1. VACUUM 실행
2. autovacuum 설정 조정
3. 쓰기 빈도가 높으면 Index-Only Scan 기대치 낮춤
```

## 핵심 정리

| 개념 | 설명 |
|-----|------|
| Visibility Map | 페이지별 all-visible 플래그 |
| all-visible | 모든 트랜잭션에 보이는 상태 |
| Index-Only Scan | Covering Index + all-visible → heap 생략 |
| Heap Fetches | all-visible이 아닌 페이지에서의 heap 접근 |
| VACUUM | dead tuple 정리 + all-visible 표시 |

## 학습 흐름

```
Lab 02b (Snapshot 기초)
        ↓
    Snapshot = 가시성 판단 기준
        ↓
Lab 09 (Visibility Map)
        ↓
    all-visible = "Snapshot 확인 불필요"
        ↓
    Index-Only Scan 최적화
```
