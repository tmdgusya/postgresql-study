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

-- ============================================
-- Part 2: 인덱스/성능 실습용 테이블 및 확장 (Lab 07~10)
-- ============================================

-- 추가 확장
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pgstattuple;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Lab 07: 인덱스 MVCC 실습용
CREATE TABLE index_mvcc_test (
    id SERIAL PRIMARY KEY,
    indexed_col INTEGER,
    non_indexed_col INTEGER,
    data TEXT
);
CREATE INDEX idx_mvcc_indexed ON index_mvcc_test(indexed_col);

-- 샘플 데이터
INSERT INTO index_mvcc_test (indexed_col, non_indexed_col, data)
SELECT i, i * 10, 'data_' || i
FROM generate_series(1, 1000) i;

-- Lab 08: JSONB/GIN 실습용
CREATE TABLE products_json (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    attributes JSONB,
    tags TEXT[],
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 샘플 JSONB 데이터
INSERT INTO products_json (name, attributes, tags, description) VALUES
    ('Laptop Pro', '{"brand": "TechCo", "specs": {"cpu": "i7", "ram": 16, "storage": "512GB"}}',
     ARRAY['electronics', 'computer', 'portable'], 'High performance laptop for professionals'),
    ('Wireless Mouse', '{"brand": "LogiTech", "specs": {"dpi": 1600, "buttons": 5}}',
     ARRAY['electronics', 'accessory', 'wireless'], 'Ergonomic wireless mouse'),
    ('Standing Desk', '{"brand": "ErgoMax", "specs": {"height_adjustable": true, "width": 120}}',
     ARRAY['furniture', 'office', 'ergonomic'], 'Height adjustable standing desk'),
    ('Mechanical Keyboard', '{"brand": "KeyMaster", "specs": {"switches": "blue", "backlit": true}}',
     ARRAY['electronics', 'accessory', 'gaming'], 'Mechanical gaming keyboard with RGB'),
    ('Monitor 27inch', '{"brand": "ViewPro", "specs": {"resolution": "4K", "refresh_rate": 144}}',
     ARRAY['electronics', 'display', 'gaming'], '27 inch 4K gaming monitor');

-- 추가 JSONB 데이터 (100건)
INSERT INTO products_json (name, attributes, tags, description)
SELECT
    'Product_' || i,
    jsonb_build_object(
        'brand', (ARRAY['TechCo', 'LogiTech', 'ErgoMax', 'KeyMaster', 'ViewPro'])[floor(random()*5+1)::int],
        'price', floor(random() * 1000 + 100)::int,
        'in_stock', random() > 0.3
    ),
    ARRAY[(ARRAY['electronics', 'furniture', 'office'])[floor(random()*3+1)::int],
          (ARRAY['premium', 'budget', 'standard'])[floor(random()*3+1)::int]],
    'Product description ' || i
FROM generate_series(1, 100) i;

-- GIN 인덱스들
CREATE INDEX idx_products_jsonb ON products_json USING gin(attributes);
CREATE INDEX idx_products_jsonb_path ON products_json USING gin(attributes jsonb_path_ops);
CREATE INDEX idx_products_tags ON products_json USING gin(tags);
CREATE INDEX idx_products_name_trgm ON products_json USING gin(name gin_trgm_ops);

-- Lab 08: BRIN 실습용 (대용량 시계열 데이터)
CREATE TABLE sensor_data (
    id SERIAL,
    sensor_id INTEGER,
    reading DECIMAL(10,2),
    recorded_at TIMESTAMP
);

-- 10만 건 시계열 데이터 (시간순 정렬 - BRIN에 최적)
INSERT INTO sensor_data (sensor_id, reading, recorded_at)
SELECT
    (i % 100) + 1,                                    -- 100개 센서
    (random() * 1000)::decimal(10,2),                 -- 0~1000 측정값
    '2024-01-01'::timestamp + (i || ' minutes')::interval  -- 시간순 정렬
FROM generate_series(1, 100000) i;

-- BRIN 인덱스 (recorded_at은 물리적으로 정렬되어 있으므로 효과적)
CREATE INDEX idx_sensor_recorded_brin ON sensor_data USING brin(recorded_at);

-- Lab 09: Index-Only Scan / Visibility Map 실습용
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    total_amount DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'pending',
    notes TEXT
);

-- 10만 건 주문 데이터
INSERT INTO orders (customer_id, order_date, total_amount, status)
SELECT
    (random() * 10000)::int + 1,                      -- 1~10000 고객
    CURRENT_DATE - (random() * 365)::int,             -- 최근 1년
    (random() * 10000)::decimal(10,2),                -- 0~10000 금액
    (ARRAY['pending', 'confirmed', 'shipped', 'delivered', 'cancelled'])[floor(random()*5+1)::int]
FROM generate_series(1, 100000);

-- Covering Index (Index-Only Scan 가능하게)
CREATE INDEX idx_orders_covering ON orders (customer_id) INCLUDE (total_amount, status);

-- Partial Index (특정 조건만 인덱싱)
CREATE INDEX idx_orders_pending ON orders (order_date) WHERE status = 'pending';

-- 인덱스 사용 현황 뷰
CREATE VIEW v_index_usage AS
SELECT
    schemaname,
    relname as table_name,
    indexrelname as index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    CASE WHEN idx_scan = 0 THEN 'UNUSED' ELSE 'USED' END as usage_status
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- 테이블/인덱스 크기 뷰
CREATE VIEW v_table_sizes AS
SELECT
    relname as table_name,
    pg_size_pretty(pg_table_size(c.oid)) as table_size,
    pg_size_pretty(pg_indexes_size(c.oid)) as indexes_size,
    pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
    (SELECT count(*) FROM pg_index WHERE indrelid = c.oid) as index_count
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = 'public' AND c.relkind = 'r'
ORDER BY pg_total_relation_size(c.oid) DESC;

-- 통계 갱신
ANALYZE accounts;
ANALYZE vacuum_test;
ANALYZE doctors_on_call;
ANALYZE index_mvcc_test;
ANALYZE products_json;
ANALYZE sensor_data;
ANALYZE orders;

-- 확인 메시지
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'PostgreSQL MVCC & Performance Lab 초기화 완료!';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE 'Part 1: MVCC 기초 (Lab 01~06)';
    RAISE NOTICE '  테이블: accounts, vacuum_test, doctors_on_call';
    RAISE NOTICE '  뷰: v_active_transactions, v_lock_waits';
    RAISE NOTICE '';
    RAISE NOTICE 'Part 2: 인덱스/성능 (Lab 07~10)';
    RAISE NOTICE '  테이블: index_mvcc_test, products_json, sensor_data, orders';
    RAISE NOTICE '  뷰: v_index_usage, v_table_sizes';
    RAISE NOTICE '';
    RAISE NOTICE '확장: pageinspect, pg_stat_statements, pgstattuple, pg_trgm';
END $$;
