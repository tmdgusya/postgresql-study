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

## 가시성 체험하기: "같은 순간, 다른 현실"

위의 예시는 "커밋 전에는 안 보이고, 커밋 후에는 보인다"는 단순한 상황입니다.
하지만 진짜 가시성의 핵심은 **커밋된 후에도 세션마다 다른 결과를 보는 것**입니다!

### 체험 1: 평행 우주 (Parallel Universes)

**핵심**: 같은 순간, 세션 A는 3건, 세션 C는 4건을 본다!

두 개의 psql 터미널을 열고 따라해보세요:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  터미널 1 (세션 A)                │  터미널 2 (세션 B)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  -- 1. 스냅샷 고정 (REPEATABLE READ)                                         │
│  BEGIN ISOLATION LEVEL                                                      │
│      REPEATABLE READ;                                                       │
│  SELECT COUNT(*) FROM accounts;                                             │
│  -- 결과: 3건                     │                                         │
│                                  │                                          │
│                                  │  -- 2. 새 데이터 추가 후 커밋             │
│                                  │  INSERT INTO accounts (name, balance)    │
│                                  │  VALUES ('Ghost', 999);                  │
│                                  │  -- (autocommit이므로 즉시 커밋)          │
│                                  │                                          │
│  -- 3. 다시 조회                  │                                          │
│  SELECT COUNT(*) FROM accounts;  │                                          │
│  -- 결과: 여전히 3건! 😱           │                                          │
│                                  │                                          │
│                                  │  -- 4. 같은 쿼리 실행                     │
│                                  │  SELECT COUNT(*) FROM accounts;          │
│                                  │  -- 결과: 4건! 🎉                         │
│                                  │                                          │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │  👆 같은 순간, 세션 A는 3건, 세션 B는 4건을 본다!                         │ │
│  │     이것이 MVCC의 "다중 버전"의 의미입니다.                               │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  COMMIT;                         │  DELETE FROM accounts                    │
│                                  │  WHERE name = 'Ghost';                   │
│  SELECT COUNT(*) FROM accounts;                                             │
│  -- 결과: 이제 4건! (새 트랜잭션)   │                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 체험 2: 유령 삭제 (Ghost Delete)

**핵심**: 다른 세션이 삭제했는데, 내 세션에서는 아직 보인다!

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  터미널 1 (세션 A)                │  터미널 2 (세션 B)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                  │                                          │
│  BEGIN ISOLATION LEVEL           │                                          │
│      REPEATABLE READ;            │                                          │
│  SELECT * FROM accounts          │                                          │
│  WHERE name = 'Alice';           │                                          │
│  -- Alice가 보인다 ✓              │                                          │
│                                  │                                          │
│                                  │  -- Alice 삭제!                          │
│                                  │  DELETE FROM accounts                    │
│                                  │  WHERE name = 'Alice';                   │
│                                  │                                          │
│  -- Alice 다시 조회               │                                          │
│  SELECT * FROM accounts          │                                          │
│  WHERE name = 'Alice';           │                                          │
│  -- Alice가 여전히 보인다! 👻      │                                          │
│                                  │                                          │
│                                  │  SELECT * FROM accounts                  │
│                                  │  WHERE name = 'Alice';                   │
│                                  │  -- 결과 없음 (삭제됨)                    │
│                                  │                                          │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │  세션 A: Alice가 보인다                                                  │ │
│  │  세션 B: Alice가 안 보인다 (삭제됨)                                       │ │
│  │  → 같은 테이블, 같은 순간, 다른 현실!                                     │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  COMMIT;                         │  -- Alice 복구                           │
│                                  │  INSERT INTO accounts (name, balance)    │
│                                  │  VALUES ('Alice', 1000);                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 체험 3: 시간 여행자 (Time Traveler)

**핵심**: 실제 DB 값은 500인데, 내 세션에서는 1000이 보인다!

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  터미널 1 (세션 A)                │  터미널 2 (세션 B)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                  │                                          │
│  BEGIN ISOLATION LEVEL           │                                          │
│      REPEATABLE READ;            │                                          │
│  SELECT balance FROM accounts    │                                          │
│  WHERE name = 'Alice';           │                                          │
│  -- 결과: 1000                   │                                          │
│                                  │                                          │
│                                  │  -- 잔액 변경                             │
│                                  │  UPDATE accounts                         │
│                                  │  SET balance = 500                       │
│                                  │  WHERE name = 'Alice';                   │
│                                  │                                          │
│  -- 다시 조회                     │                                          │
│  SELECT balance FROM accounts    │                                          │
│  WHERE name = 'Alice';           │                                          │
│  -- 결과: 여전히 1000! ⏰          │                                          │
│                                  │                                          │
│                                  │  SELECT balance FROM accounts            │
│                                  │  WHERE name = 'Alice';                   │
│                                  │  -- 결과: 500                            │
│                                  │                                          │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │  세션 A: "1000원이에요" (과거에 갇힘)                                     │ │
│  │  세션 B: "500원이에요" (현재)                                            │ │
│  │  → 세션 A는 시간 여행자처럼 과거의 데이터를 보고 있습니다!                 │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  COMMIT;                         │  -- 원상 복구                            │
│                                  │  UPDATE accounts                         │
│                                  │  SET balance = 1000                      │
│                                  │  WHERE name = 'Alice';                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 왜 이런 일이 일어날까?

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         REPEATABLE READ의 동작                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   세션 A                                세션 B                              │
│   ┌──────────────┐                     ┌──────────────┐                    │
│   │ 스냅샷 #1    │                     │              │                    │
│   │ (과거 고정)  │ ◄─────────────────── │ 변경 + 커밋  │                    │
│   │              │   변경이 안 보임!    │              │                    │
│   └──────────────┘                     └──────────────┘                    │
│                                                                             │
│   • 세션 A는 트랜잭션 시작 시점의 스냅샷을 "고정"                             │
│   • 세션 B가 아무리 많이 변경하고 커밋해도, 세션 A의 스냅샷은 변하지 않음      │
│   • 마치 시간이 멈춘 것처럼 과거의 데이터를 계속 봄                            │
│                                                                             │
│   이것이 MVCC의 "Multi-Version"의 의미:                                      │
│   같은 데이터의 여러 버전이 동시에 존재하고, 각 세션은 자신의 버전을 봄        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 자동화된 실습

위 시나리오들을 자동으로 실행해보고 싶다면:
```bash
python labs/lab00_visibility_experience.py
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

### 실제로 죽어있는 튜플/살아있는 튜플을 보는 방법
```sql
SELECT 
    lp as item,
    t_xmin as xmin,
    t_xmax as xmax,
    t_ctid as ctid,
    CASE 
        WHEN t_xmax = 0 THEN '🟢 LIVE'
        ELSE '💀 DEAD (or being deleted)'
    END as status,
    CASE 
        WHEN (t_infomask & 256) > 0 THEN 'COMMITTED'
        WHEN (t_infomask & 512) > 0 THEN 'ABORTED'  
        ELSE 'IN_PROGRESS'
    END as xmin_committed
FROM heap_page_items(get_raw_page('accounts', 0))
WHERE t_data IS NOT NULL;
```

---

## 다음 단계

이 개념들을 실습을 통해 직접 확인해보세요:

### 먼저 해보세요! (강력 추천)
```bash
python labs/lab00_visibility_experience.py
```
**가시성의 "아하!" 순간**을 체험할 수 있는 대화형 실습입니다.
"커밋됐는데 왜 안 보여요?"라는 질문의 답을 찾게 됩니다.

### 심화 실습
1. `lab01_xmin_xmax.py` - xmin, xmax 기초 실습
2. `lab02_update_delete.py` - UPDATE/DELETE 시 변화 관찰
3. `lab02b_snapshot.py` - 스냅샷 구조 상세 분석
4. `lab03_isolation.py` - 격리 수준별 동작 비교
5. `lab04_concurrent.py` - 동시 쓰기 시나리오
6. `lab05_vacuum.py` - VACUUM과 dead tuple
7. `lab06_locks.py` - Lock 모니터링
