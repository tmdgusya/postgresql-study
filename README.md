# PostgreSQL MVCC & Performance 학습 프로젝트

PostgreSQL의 MVCC(Multi-Version Concurrency Control) 내부 동작과 인덱스/성능 최적화를 실습을 통해 이해하기 위한 프로젝트입니다.

## 학습 목표

### Part 1: MVCC 기초 (Lab 01~06)
- 튜플 시스템 컬럼(xmin, xmax, ctid)의 의미와 변화 이해
- **Snapshot의 구조와 가시성 규칙 이해**
- 트랜잭션 격리 수준별 동작 차이 체험
- 동시성 제어 메커니즘 파악
- VACUUM과 Lock 모니터링 방법 습득

### Part 2: 인덱스와 성능 (Lab 07~10)
- 인덱스와 MVCC의 상호작용 이해
- B-tree, GIN, BRIN 인덱스 활용
- **Index-Only Scan과 Visibility Map** (Snapshot 연결!)
- 쿼리 실행 계획 분석 및 최적화
- matplotlib로 성능 시각화

## 빠른 시작

### 1. 환경 시작

```bash
# PostgreSQL 컨테이너 시작
docker-compose up -d

# 상태 확인
docker-compose ps

# 로그 확인
docker-compose logs -f postgres
```

### 2. Python 환경 설정

```bash
# 가상환경 생성 (권장)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 또는 venv\Scripts\activate  # Windows

# 의존성 설치
pip install -r labs/requirements.txt
```

### 3. 실습 시작

```bash
# Part 1: MVCC 기초 (스냅샷 이해가 격리 수준 이해의 핵심!)
python labs/lab01_xmin_xmax.py      # 튜플 시스템 컬럼
python labs/lab02_update_delete.py  # UPDATE/DELETE 동작
python labs/lab02b_snapshot.py      # ★ Snapshot 이해 (중요!)
python labs/lab03_isolation.py      # 격리 수준 비교
python labs/lab04_concurrent.py     # 동시 쓰기
python labs/lab05_vacuum.py         # VACUUM
python labs/lab06_locks.py          # Lock 모니터링

# Part 2: 인덱스와 성능 (VACUUM 후 컨테이너 재시작 권장)
docker-compose down && docker-compose up -d

python labs/lab07_index_mvcc.py     # 인덱스와 MVCC 상호작용
python labs/lab08_index_types.py    # B-tree, GIN, BRIN 인덱스
python labs/lab09_query_plan.py     # ★ 실행 계획과 Visibility Map
python labs/lab10_monitoring.py     # 성능 모니터링 (matplotlib)
```

## 프로젝트 구조

```
postgresql-study/
├── docker-compose.yml          # PostgreSQL 컨테이너 설정 (pg_stat_statements 포함)
├── README.md                   # 이 문서
├── docs/
│   ├── 01-mvcc-basics.md       # MVCC 기초 개념 설명
│   ├── 02-snapshot.md          # ★ Snapshot 개념 (핵심!)
│   ├── 03-index-fundamentals.md # 인덱스와 MVCC 상호작용
│   └── 04-visibility-map.md    # ★ Visibility Map (Snapshot 연결!)
├── scripts/
│   └── init.sql                # DB 초기화 (테이블, 뷰, 확장)
└── labs/
    ├── requirements.txt        # Python 의존성 (psycopg2, tabulate, matplotlib)
    │
    │   # Part 1: MVCC 기초
    ├── lab01_xmin_xmax.py      # xmin/xmax 기초
    ├── lab02_update_delete.py  # UPDATE/DELETE 동작
    ├── lab02b_snapshot.py      # ★ Snapshot 실습
    ├── lab03_isolation.py      # 격리 수준 비교
    ├── lab04_concurrent.py     # 동시 쓰기 시나리오
    ├── lab05_vacuum.py         # VACUUM과 dead tuple
    ├── lab06_locks.py          # Lock 모니터링
    │
    │   # Part 2: 인덱스와 성능
    ├── lab07_index_mvcc.py     # 인덱스와 MVCC, HOT UPDATE
    ├── lab08_index_types.py    # B-tree, GIN, BRIN 인덱스
    ├── lab09_query_plan.py     # 실행 계획, Index-Only Scan
    └── lab10_monitoring.py     # pg_stat_statements, 시각화
```

## 실습 가이드

### Lab 01: xmin/xmax 기초

- PostgreSQL 튜플의 숨겨진 시스템 컬럼 확인
- 커밋 전후 데이터 가시성 차이 체험
- 트랜잭션 ID 동작 원리 이해

### Lab 02: UPDATE/DELETE 동작

- UPDATE가 내부적으로 DELETE + INSERT임을 확인
- pageinspect로 raw 튜플 데이터 직접 관찰
- HOT(Heap-Only Tuple) UPDATE 개념

### Lab 02b: Snapshot 이해 (핵심!)

- **Snapshot의 구조**: xmin, xmax, xip[] 세 가지 구성 요소
- **가시성 규칙**: 스냅샷으로 튜플이 보이는지 판단하는 방법
- **격리 수준과의 관계**: 스냅샷 생성 시점이 격리 수준을 결정
- pg_current_snapshot() 함수로 실시간 스냅샷 확인

### Lab 03: 격리 수준 비교

- READ COMMITTED vs REPEATABLE READ 차이
- Non-Repeatable Read / Phantom Read 현상
- Write Skew 문제와 SERIALIZABLE 해결

### Lab 04: 동시 쓰기

- Row-level lock으로 Lost Update 방지
- SELECT FOR UPDATE 패턴
- Optimistic Locking 구현

### Lab 05: VACUUM

- Dead tuple 생성 및 확인
- VACUUM vs VACUUM FULL 차이
- Autovacuum 설정 이해

### Lab 06: Lock 모니터링

- PostgreSQL Lock 유형 이해
- pg_locks로 락 상태 확인
- Deadlock 발생 및 해결 과정 관찰

---

### Lab 07: 인덱스와 MVCC 상호작용

- **인덱스에는 MVCC 정보가 없다** - ctid만 저장
- B-tree 인덱스 내부 구조 탐색 (bt_page_items)
- **HOT UPDATE** - 인덱스 bloat 방지 메커니즘
- 인덱스 컬럼 변경 시 새 인덱스 엔트리 생성 확인

### Lab 08: 인덱스 유형과 활용

- **B-tree**: 등호/범위/정렬 쿼리 최적화
- **GIN**: JSONB, 배열, 전문검색 (pg_trgm)
- **BRIN**: 대용량 시계열 데이터 (100배 작은 크기)
- 인덱스 유형별 쿼리 패턴 매칭

### Lab 09: 실행 계획과 Visibility Map ★

- EXPLAIN ANALYZE 출력 해석
- **Visibility Map과 Index-Only Scan** 관계
- `Heap Fetches` 의미와 최적화 방법
- **Covering Index** 설계 (INCLUDE 활용)
- Snapshot → Visibility Map 핵심 연결 이해

### Lab 10: 성능 모니터링과 튜닝

- **pg_stat_statements** - 쿼리별 실행 통계
- **pgstattuple** - 테이블/인덱스 bloat 분석
- **matplotlib** 시각화 - 쿼리 성능 그래프
- 인덱스 사용량 분석 (v_index_usage)

## 직접 SQL로 실습하기

```bash
# psql로 접속
docker exec -it pg-mvcc-study psql -U study -d mvcc_lab

# 또는 외부 클라이언트 사용
psql -h localhost -p 5432 -U study -d mvcc_lab
# 비밀번호: study123
```

### 유용한 쿼리

```sql
-- 시스템 컬럼 확인
SELECT xmin, xmax, ctid, * FROM accounts;

-- 현재 트랜잭션 ID
SELECT txid_current();

-- ★ 현재 스냅샷 확인 (xmin:xmax:xip_list 형식)
SELECT pg_current_snapshot();

-- ★ 스냅샷 구성 요소 분해
SELECT
    pg_snapshot_xmin(pg_current_snapshot()) as snap_xmin,
    pg_snapshot_xmax(pg_current_snapshot()) as snap_xmax;

-- Dead tuple 확인
SELECT relname, n_live_tup, n_dead_tup
FROM pg_stat_user_tables;

-- 현재 락 상태
SELECT * FROM pg_locks WHERE relation = 'accounts'::regclass;

-- 락 대기 상황
SELECT * FROM v_lock_waits;

-- pageinspect로 raw 튜플 확인
SELECT * FROM heap_page_items(get_raw_page('accounts', 0));

-- ============================================
-- Part 2: 인덱스/성능 쿼리
-- ============================================

-- 인덱스 사용 현황
SELECT * FROM v_index_usage;

-- 테이블/인덱스 크기
SELECT * FROM v_table_sizes;

-- B-tree 인덱스 내부 구조
SELECT * FROM bt_metap('idx_mvcc_indexed');
SELECT * FROM bt_page_items('idx_mvcc_indexed', 1);

-- Visibility Map 상태 확인
SELECT * FROM pg_visibility('orders');

-- Index-Only Scan 확인 (Heap Fetches 주목!)
EXPLAIN (ANALYZE, BUFFERS)
SELECT customer_id, total_amount FROM orders WHERE customer_id = 100;

-- 쿼리 통계 (가장 느린 쿼리)
SELECT query, calls, mean_exec_time, total_exec_time
FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;

-- 테이블 bloat 분석
SELECT * FROM pgstattuple('orders');
```

## 정리

```bash
# 컨테이너 중지
docker-compose down

# 볼륨까지 삭제 (데이터 초기화)
docker-compose down -v
```

## 참고 자료

### Part 1: MVCC
- [PostgreSQL MVCC 공식 문서](https://www.postgresql.org/docs/current/mvcc.html)
- [Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html)
- [Explicit Locking](https://www.postgresql.org/docs/current/explicit-locking.html)
- [VACUUM](https://www.postgresql.org/docs/current/routine-vacuuming.html)

### Part 2: 인덱스와 성능
- [Indexes](https://www.postgresql.org/docs/current/indexes.html)
- [Index Types](https://www.postgresql.org/docs/current/indexes-types.html)
- [EXPLAIN 사용법](https://www.postgresql.org/docs/current/using-explain.html)
- [pg_stat_statements](https://www.postgresql.org/docs/current/pgstatstatements.html)
- [Visibility Map](https://www.postgresql.org/docs/current/storage-vm.html)
