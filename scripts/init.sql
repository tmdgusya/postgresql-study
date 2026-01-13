-- PostgreSQL MVCC 학습을 위한 초기화 스크립트
-- ============================================

-- pageinspect 확장: 튜플의 내부 구조를 직접 확인할 수 있게 해줌
CREATE EXTENSION IF NOT EXISTS pageinspect;

-- 기본 실습용 테이블: accounts (은행 계좌)
CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    balance INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 초기 데이터 삽입
INSERT INTO accounts (name, balance) VALUES
    ('Alice', 1000),
    ('Bob', 2000),
    ('Charlie', 3000);

-- VACUUM/Dead Tuple 실습용 테이블
CREATE TABLE vacuum_test (
    id SERIAL PRIMARY KEY,
    data VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Lock 실습용 테이블: 의사 당직 스케줄 (Write Skew 예제용)
CREATE TABLE doctors_on_call (
    id SERIAL PRIMARY KEY,
    doctor_name VARCHAR(100) NOT NULL,
    shift_date DATE NOT NULL,
    is_on_call BOOLEAN NOT NULL DEFAULT true
);

INSERT INTO doctors_on_call (doctor_name, shift_date, is_on_call) VALUES
    ('Dr. Kim', CURRENT_DATE, true),
    ('Dr. Lee', CURRENT_DATE, true);

-- 유용한 뷰: 현재 실행 중인 트랜잭션 확인
CREATE VIEW v_active_transactions AS
SELECT
    pid,
    usename,
    application_name,
    state,
    backend_xid,
    backend_xmin,
    query_start,
    LEFT(query, 100) as query_preview
FROM pg_stat_activity
WHERE state != 'idle'
  AND pid != pg_backend_pid();

-- 유용한 뷰: 락 대기 상황 확인
CREATE VIEW v_lock_waits AS
SELECT
    blocked.pid AS blocked_pid,
    blocked.usename AS blocked_user,
    LEFT(blocked.query, 60) AS blocked_query,
    blocking.pid AS blocking_pid,
    blocking.usename AS blocking_user,
    LEFT(blocking.query, 60) AS blocking_query
FROM pg_stat_activity blocked
JOIN pg_locks blocked_locks ON blocked.pid = blocked_locks.pid AND NOT blocked_locks.granted
JOIN pg_locks blocking_locks ON blocked_locks.locktype = blocking_locks.locktype
    AND blocked_locks.database IS NOT DISTINCT FROM blocking_locks.database
    AND blocked_locks.relation IS NOT DISTINCT FROM blocking_locks.relation
    AND blocked_locks.page IS NOT DISTINCT FROM blocking_locks.page
    AND blocked_locks.tuple IS NOT DISTINCT FROM blocking_locks.tuple
    AND blocked_locks.virtualxid IS NOT DISTINCT FROM blocking_locks.virtualxid
    AND blocked_locks.transactionid IS NOT DISTINCT FROM blocking_locks.transactionid
    AND blocked_locks.classid IS NOT DISTINCT FROM blocking_locks.classid
    AND blocked_locks.objid IS NOT DISTINCT FROM blocking_locks.objid
    AND blocked_locks.objsubid IS NOT DISTINCT FROM blocking_locks.objsubid
    AND blocked_locks.pid != blocking_locks.pid
JOIN pg_stat_activity blocking ON blocking_locks.pid = blocking.pid
WHERE blocking_locks.granted;

-- 테이블 통계 갱신
ANALYZE accounts;
ANALYZE vacuum_test;
ANALYZE doctors_on_call;

-- 확인 메시지
DO $$
BEGIN
    RAISE NOTICE 'MVCC Lab 초기화 완료!';
    RAISE NOTICE '테이블: accounts, vacuum_test, doctors_on_call';
    RAISE NOTICE '뷰: v_active_transactions, v_lock_waits';
END $$;
