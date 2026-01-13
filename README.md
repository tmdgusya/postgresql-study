# PostgreSQL MVCC 학습 프로젝트

PostgreSQL의 MVCC(Multi-Version Concurrency Control) 내부 동작을 실습을 통해 이해하기 위한 프로젝트입니다.

## 학습 목표

- 튜플 시스템 컬럼(xmin, xmax, ctid)의 의미와 변화 이해
- **Snapshot의 구조와 가시성 규칙 이해**
- 트랜잭션 격리 수준별 동작 차이 체험
- 동시성 제어 메커니즘 파악
- VACUUM과 Lock 모니터링 방법 습득

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
# Lab 순서대로 실행 (스냅샷 이해가 격리 수준 이해의 핵심!)
python labs/lab01_xmin_xmax.py      # 튜플 시스템 컬럼
python labs/lab02_update_delete.py  # UPDATE/DELETE 동작
python labs/lab02b_snapshot.py      # ★ Snapshot 이해 (중요!)
python labs/lab03_isolation.py      # 격리 수준 비교
python labs/lab04_concurrent.py     # 동시 쓰기
python labs/lab05_vacuum.py         # VACUUM
python labs/lab06_locks.py          # Lock 모니터링
```

## 프로젝트 구조

```
postgresql-study/
├── docker-compose.yml          # PostgreSQL 컨테이너 설정
├── README.md                   # 이 문서
├── docs/
│   ├── 01-mvcc-basics.md       # MVCC 기초 개념 설명
│   └── 02-snapshot.md          # ★ Snapshot 개념 (핵심!)
├── scripts/
│   └── init.sql                # DB 초기화 (테이블, 뷰, 확장)
└── labs/
    ├── requirements.txt        # Python 의존성
    ├── lab01_xmin_xmax.py      # xmin/xmax 기초
    ├── lab02_update_delete.py  # UPDATE/DELETE 동작
    ├── lab02b_snapshot.py      # ★ Snapshot 실습
    ├── lab03_isolation.py      # 격리 수준 비교
    ├── lab04_concurrent.py     # 동시 쓰기 시나리오
    ├── lab05_vacuum.py         # VACUUM과 dead tuple
    └── lab06_locks.py          # Lock 모니터링
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
```

## 정리

```bash
# 컨테이너 중지
docker-compose down

# 볼륨까지 삭제 (데이터 초기화)
docker-compose down -v
```

## 참고 자료

- [PostgreSQL MVCC 공식 문서](https://www.postgresql.org/docs/current/mvcc.html)
- [Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html)
- [Explicit Locking](https://www.postgresql.org/docs/current/explicit-locking.html)
- [VACUUM](https://www.postgresql.org/docs/current/routine-vacuuming.html)
