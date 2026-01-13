# PostgreSQL MVCC 기초 개념

## MVCC란?

**MVCC (Multi-Version Concurrency Control)**는 PostgreSQL이 동시성을 처리하는 핵심 메커니즘입니다.

핵심 아이디어: **읽기는 쓰기를 블록하지 않고, 쓰기도 읽기를 블록하지 않습니다.**

이를 위해 PostgreSQL은 데이터의 여러 버전을 동시에 유지합니다.

---

## 튜플 시스템 컬럼

모든 PostgreSQL 테이블의 각 row(튜플)에는 숨겨진 시스템 컬럼들이 있습니다:

### xmin (트랜잭션 ID - 생성)
```sql
SELECT xmin, * FROM accounts;
```
- 이 튜플을 **INSERT한 트랜잭션의 ID**
- 해당 트랜잭션이 커밋되어야 다른 트랜잭션에서 이 튜플을 볼 수 있음

### xmax (트랜잭션 ID - 삭제/수정)
```sql
SELECT xmax, * FROM accounts;
```
- 이 튜플을 **DELETE 또는 UPDATE한 트랜잭션의 ID**
- `0`이면 아직 삭제/수정되지 않은 "살아있는" 튜플
- UPDATE 시: 기존 튜플의 xmax가 설정되고, 새 튜플이 생성됨

### ctid (튜플 위치)
```sql
SELECT ctid, * FROM accounts;
```
- 형식: `(page_number, tuple_offset)`
- 예: `(0, 1)` = 0번 페이지의 1번째 튜플
- UPDATE 시 ctid가 변경됨 (새 위치로 이동)

---

## Visibility (가시성) 규칙

트랜잭션이 튜플을 볼 수 있는지는 다음 규칙으로 결정됩니다:

### 기본 규칙
```
튜플이 보이려면:
1. xmin 트랜잭션이 커밋되어야 함
2. xmax가 0이거나, xmax 트랜잭션이 아직 커밋되지 않았거나 abort됨
```

### 예시 시나리오

```
시간 →

T1 (xid=100): BEGIN → INSERT (xmin=100) → COMMIT
T2 (xid=101): ─────────────────────────────→ SELECT (볼 수 있음!)

T1 (xid=100): BEGIN → INSERT (xmin=100) → (아직 커밋 안함)
T2 (xid=101): ─────────────────────────→ SELECT (안 보임!)
```

---

## UPDATE의 내부 동작

PostgreSQL에서 **UPDATE는 실제로 DELETE + INSERT**입니다:

```
UPDATE accounts SET balance = 500 WHERE id = 1;

내부적으로:
1. 기존 튜플: xmax = 현재 트랜잭션 ID (삭제 표시)
2. 새 튜플: xmin = 현재 트랜잭션 ID, 새로운 ctid
```

### 왜 이렇게 동작할까?

1. **MVCC 지원**: 다른 트랜잭션은 여전히 이전 버전을 볼 수 있음
2. **롤백 용이**: 커밋 전에는 xmax만 설정된 상태이므로 롤백 시 원복 가능
3. **락 최소화**: 읽기 트랜잭션을 블록하지 않음

---

## 트랜잭션 격리 수준

### READ COMMITTED (기본값)
```sql
BEGIN;  -- 또는 BEGIN ISOLATION LEVEL READ COMMITTED;
```
- 각 **쿼리 시작 시점**의 커밋된 데이터를 봄
- 같은 트랜잭션 내에서도 다른 쿼리는 다른 데이터를 볼 수 있음 (Non-Repeatable Read)

### REPEATABLE READ
```sql
BEGIN ISOLATION LEVEL REPEATABLE READ;
```
- **트랜잭션 시작 시점**의 스냅샷을 고정
- 트랜잭션 동안 일관된 데이터를 봄
- 단, 동시 UPDATE 시 직렬화 오류 발생 가능

### SERIALIZABLE
```sql
BEGIN ISOLATION LEVEL SERIALIZABLE;
```
- 트랜잭션들이 **순차적으로 실행된 것처럼** 동작
- 가장 엄격하지만, 충돌 시 롤백/재시도 필요
- Write Skew 같은 이상 현상 방지

---

## Dead Tuple과 VACUUM

### Dead Tuple이란?
- UPDATE/DELETE 후 더 이상 필요 없지만 아직 물리적으로 존재하는 튜플
- xmax가 설정된 "삭제된" 튜플들

### VACUUM의 역할
```sql
VACUUM accounts;
VACUUM VERBOSE accounts;  -- 상세 정보 출력
```
- Dead tuple을 정리하여 공간 재사용 가능하게 함
- `autovacuum`이 자동으로 실행되지만, 수동 실행도 가능

### 확인 방법
```sql
SELECT n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname = 'accounts';
```

---

## 다음 단계

이 개념들을 실습을 통해 직접 확인해보세요:

1. `lab01_xmin_xmax.py` - xmin, xmax 기초 실습
2. `lab02_update_delete.py` - UPDATE/DELETE 시 변화 관찰
3. `lab03_isolation.py` - 격리 수준별 동작 비교
4. `lab04_concurrent.py` - 동시 쓰기 시나리오
5. `lab05_vacuum.py` - VACUUM과 dead tuple
6. `lab06_locks.py` - Lock 모니터링
