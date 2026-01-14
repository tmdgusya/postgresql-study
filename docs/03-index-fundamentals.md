# 인덱스 기초와 MVCC

## 개요

PostgreSQL 인덱스가 MVCC와 어떻게 상호작용하는지 이해합니다.

## 핵심 개념

### 1. 인덱스와 ctid

```
인덱스 구조:
┌─────────────────────────────────────────────┐
│ B-tree Index (예: idx_on_name)              │
├─────────────────────────────────────────────┤
│  Key Value    →   ctid (page, offset)      │
│  ──────────       ─────────────────         │
│  "Alice"      →   (0, 1)                   │
│  "Bob"        →   (0, 2)                   │
│  "Charlie"    →   (1, 1)                   │
└─────────────────────────────────────────────┘

★ 인덱스는 ctid(물리적 위치)만 저장
★ xmin, xmax 같은 MVCC 정보는 없음!
```

### 2. 인덱스에 MVCC 정보가 없는 이유

인덱스는 **데이터의 위치**만 알려줍니다. 실제 가시성 판단은 heap tuple의 xmin/xmax를 확인해야 합니다.

```
쿼리 실행 흐름:
1. Index Scan → ctid 획득
2. Heap 접근 → 해당 ctid의 tuple 읽기
3. MVCC 가시성 검사 → xmin/xmax 확인
4. 결과 반환 (또는 skip)
```

## 인덱스 유형별 특성

### B-tree (기본)

| 특성 | 설명 |
|-----|------|
| 용도 | 등호, 범위, 정렬 |
| 연산자 | =, <, >, <=, >=, BETWEEN |
| 정렬 | 자동 정렬됨 (ORDER BY 최적화) |
| 복합 키 | 지원 (컬럼 순서 중요!) |

### GIN (Generalized Inverted Index)

| 특성 | 설명 |
|-----|------|
| 용도 | JSONB, 배열, 전문검색 |
| 연산자 | @>, ?, ?&, ?|, @@ |
| 구조 | 역인덱스 (값 → 행 목록) |
| 갱신 | 느림 (pending list 사용) |

### BRIN (Block Range Index)

| 특성 | 설명 |
|-----|------|
| 용도 | 대용량 시계열 데이터 |
| 연산자 | =, <, > 등 범위 연산 |
| 크기 | 매우 작음 (B-tree의 1/100) |
| 조건 | 데이터가 물리적으로 정렬되어 있어야 효과적 |

## HOT UPDATE (Heap-Only Tuple)

인덱스 컬럼이 변경되지 않으면 인덱스 갱신을 생략할 수 있습니다.

```
HOT UPDATE 조건:
1. 인덱스 컬럼이 변경되지 않음
2. 새 tuple이 같은 페이지에 들어갈 공간 있음
3. 기존 tuple에서 새 tuple로 "forwarding pointer" 설정

장점:
- 인덱스 갱신 생략 → 쓰기 성능 향상
- 인덱스 bloat 감소
```

## 실습 연결

| Lab | 학습 내용 |
|-----|----------|
| Lab 07 | 인덱스와 ctid, HOT UPDATE |
| Lab 08 | B-tree, GIN, BRIN 심화 |
| Lab 09 | Index-Only Scan, Visibility Map |

## 핵심 정리

1. **인덱스는 ctid만 저장** - MVCC 정보 없음
2. **모든 인덱스 접근은 heap 확인 필요** (Index-Only Scan 제외)
3. **HOT UPDATE**로 인덱스 갱신 최소화 가능
4. **인덱스 유형 선택**은 쿼리 패턴에 따라 결정
