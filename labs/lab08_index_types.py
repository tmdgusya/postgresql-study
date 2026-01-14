#!/usr/bin/env python3
"""
Lab 08: 인덱스 유형과 활용
========================

학습 목표:
- B-tree, GIN, BRIN 인덱스의 내부 구조 이해
- 각 인덱스 유형의 적합한 사용 사례 파악
- 인덱스 선택 기준 수립

선수 지식: Lab 07 (인덱스 기초와 MVCC)

사용 테이블:
- products_json: JSONB, 배열 컬럼 (GIN 실습)
- sensor_data: 10만 건 시계열 데이터 (BRIN 실습)
- orders: 10만 건 주문 데이터 (B-tree 실습)
"""

import psycopg2
from psycopg2 import sql
from tabulate import tabulate
import time

# 데이터베이스 연결 설정
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'mvcc_lab',
    'user': 'study',
    'password': 'study123'
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def print_section(title):
    print(f"\n{'='*70}")
    print(f" {title}")
    print('='*70)


def print_subsection(title):
    print(f"\n--- {title} ---")


def execute_and_show(cur, query, description=""):
    """쿼리 실행 후 결과 출력"""
    if description:
        print(f"\n>> {description}")
    print(f"SQL: {query[:100]}..." if len(query) > 100 else f"SQL: {query}")

    start_time = time.time()
    cur.execute(query)
    elapsed = (time.time() - start_time) * 1000

    if cur.description:
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
        print(tabulate(rows, headers=headers, tablefmt='psql'))
        print(f"({len(rows)}개 행, {elapsed:.2f}ms)")
        return rows
    else:
        print(f"완료 ({elapsed:.2f}ms)")
        return None


def get_explain_analyze(cur, query):
    """EXPLAIN ANALYZE 결과 반환"""
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}")
    return '\n'.join([row[0] for row in cur.fetchall()])


# =============================================================================
# 시나리오 1: B-tree 인덱스 심화
# =============================================================================

def scenario_1_btree_advanced():
    """
    시나리오 1: B-tree 인덱스 심화

    B-tree는 PostgreSQL의 기본 인덱스 유형입니다.
    - 등호(=), 범위(<, >, BETWEEN), 정렬(ORDER BY)에 효율적
    - 복합 인덱스에서 컬럼 순서가 매우 중요!
    """
    print_section("시나리오 1: B-tree 인덱스 심화")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1-1: 범위 쿼리 성능
        print_subsection("1-1: B-tree와 범위 쿼리")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ B-tree 구조와 범위 쿼리                                          │
├─────────────────────────────────────────────────────────────────┤
│                    [Root]                                        │
│                   /      \\                                       │
│            [Branch]      [Branch]                                │
│            /     \\       /     \\                                 │
│       [Leaf] → [Leaf] → [Leaf] → [Leaf]  (← 리프 노드는 연결됨)  │
│                                                                  │
│ ★ 범위 쿼리 시:                                                   │
│   1. 시작점을 B-tree 탐색으로 빠르게 찾음                         │
│   2. 연결된 리프 노드를 순차 스캔                                  │
│   3. 종료 조건까지 탐색                                           │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 인덱스 생성 (없으면)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_date
            ON orders(order_date)
        """)
        conn.commit()

        # 범위 쿼리 실행
        query = """
            SELECT COUNT(*), MIN(order_date), MAX(order_date)
            FROM orders
            WHERE order_date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
        """

        print("\n범위 쿼리 실행 계획:")
        explain = get_explain_analyze(cur, query)
        print(explain)

        execute_and_show(cur, query, "최근 30일 주문 집계")

        # 1-2: 복합 인덱스와 컬럼 순서
        print_subsection("1-2: 복합 인덱스 - 컬럼 순서의 중요성")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ 복합 인덱스 컬럼 순서 규칙                                        │
├─────────────────────────────────────────────────────────────────┤
│ CREATE INDEX idx ON orders(customer_id, status, order_date)     │
│                                                                  │
│ ✓ 사용 가능한 쿼리 패턴:                                         │
│   - WHERE customer_id = ?                          (첫 컬럼)    │
│   - WHERE customer_id = ? AND status = ?           (첫 두 컬럼) │
│   - WHERE customer_id = ? AND status = ? AND ...   (전체)       │
│                                                                  │
│ ✗ 인덱스 활용 불가:                                              │
│   - WHERE status = ?                    (첫 컬럼 없음)          │
│   - WHERE order_date = ?                (중간 컬럼 건너뜀)       │
│                                                                  │
│ ★ "왼쪽부터 연속으로" 사용해야 함!                                 │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 복합 인덱스 생성
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_composite
            ON orders(customer_id, status, order_date)
        """)
        conn.commit()

        # 좋은 예: 첫 번째 컬럼 사용
        query_good = """
            SELECT id, customer_id, status, order_date
            FROM orders
            WHERE customer_id = 100
            LIMIT 5
        """
        print("\n[좋은 예] customer_id(첫 컬럼) 조건:")
        print(get_explain_analyze(cur, query_good))

        # 나쁜 예: 첫 번째 컬럼 건너뜀
        query_bad = """
            SELECT id, customer_id, status, order_date
            FROM orders
            WHERE status = 'pending'
            LIMIT 5
        """
        print("\n[나쁜 예] status만 조건 (첫 컬럼 건너뜀):")
        print(get_explain_analyze(cur, query_bad))

        # 1-3: 정렬과 B-tree
        print_subsection("1-3: B-tree를 활용한 정렬")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ B-tree와 ORDER BY                                                │
├─────────────────────────────────────────────────────────────────┤
│ B-tree 리프 노드는 정렬된 상태로 연결되어 있음                    │
│                                                                  │
│ → 인덱스 컬럼으로 ORDER BY 시 추가 정렬 불필요!                   │
│ → 인덱스 역순 스캔(DESC)도 가능                                  │
│                                                                  │
│ 예: CREATE INDEX idx ON orders(order_date)                       │
│     SELECT * FROM orders ORDER BY order_date         -- 정순    │
│     SELECT * FROM orders ORDER BY order_date DESC    -- 역순    │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 정렬 쿼리 (인덱스 활용)
        query_sorted = """
            SELECT id, order_date, total_amount
            FROM orders
            ORDER BY order_date DESC
            LIMIT 10
        """
        print("\nORDER BY order_date DESC (인덱스 역순 스캔):")
        print(get_explain_analyze(cur, query_sorted))

        print("""
★ 핵심 정리:
  1. B-tree는 범위 쿼리와 정렬에 최적화
  2. 복합 인덱스는 "왼쪽부터 연속으로" 사용해야 효과적
  3. ORDER BY도 인덱스로 최적화 가능 (정순/역순 모두)
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 2: GIN 인덱스 - JSONB
# =============================================================================

def scenario_2_gin_jsonb():
    """
    시나리오 2: GIN 인덱스와 JSONB

    GIN(Generalized Inverted Index)은 "역인덱스" 구조입니다.
    - JSONB 내부의 키/값을 검색할 때 사용
    - @>, ?, ?|, ?& 연산자 지원
    """
    print_section("시나리오 2: GIN 인덱스 - JSONB")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ GIN (Generalized Inverted Index) 구조                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 일반 B-tree:  Row → Values                                       │
│   Row1 → {"brand": "TechCo", "price": 500}                       │
│   Row2 → {"brand": "LogiTech", "price": 300}                     │
│                                                                  │
│ GIN (역인덱스):  Value → Rows                                     │
│   "brand"="TechCo"   → [Row1, Row5, Row10, ...]                  │
│   "brand"="LogiTech" → [Row2, Row7, Row15, ...]                  │
│   "price"=500        → [Row1, Row3, Row8, ...]                   │
│                                                                  │
│ ★ 특정 키/값이 포함된 행을 빠르게 찾을 수 있음!                   │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 2-1: @> (contains) 연산자
        print_subsection("2-1: @> (Contains) 연산자")

        # 현재 인덱스 확인
        execute_and_show(cur, """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'products_json'
            AND indexname LIKE '%jsonb%'
        """, "products_json의 JSONB 인덱스들")

        # @> 연산자 사용
        query_contains = """
            SELECT id, name, attributes->>'brand' as brand
            FROM products_json
            WHERE attributes @> '{"brand": "TechCo"}'
            LIMIT 5
        """

        print("\n@> 연산자로 brand='TechCo' 검색:")
        print(get_explain_analyze(cur, query_contains))
        execute_and_show(cur, query_contains)

        # 2-2: ? (exists) 연산자
        print_subsection("2-2: ? (Exists) 연산자")

        query_exists = """
            SELECT id, name, attributes
            FROM products_json
            WHERE attributes ? 'specs'
            LIMIT 5
        """

        print("\n? 연산자로 'specs' 키 존재 여부 검색:")
        print(get_explain_analyze(cur, query_exists))
        execute_and_show(cur, query_exists)

        # 2-3: jsonb_path_ops vs 기본 ops 비교
        print_subsection("2-3: jsonb_path_ops vs 기본 GIN ops")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ GIN 연산자 클래스 비교                                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 기본 (jsonb_ops):                                                │
│   - 지원: @>, ?, ?|, ?&                                          │
│   - 키 존재 여부 검색 가능                                        │
│   - 인덱스 크기 더 큼                                             │
│                                                                  │
│ jsonb_path_ops:                                                  │
│   - 지원: @> 만!                                                  │
│   - 키 존재 여부 검색 불가                                        │
│   - 인덱스 크기 더 작음 (약 30% 감소)                             │
│   - @> 쿼리 성능 더 좋음                                          │
│                                                                  │
│ ★ 선택 기준:                                                     │
│   - @> 쿼리만 사용 → jsonb_path_ops                              │
│   - ?, ?|, ?& 필요 → 기본 jsonb_ops                              │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 인덱스 크기 비교
        execute_and_show(cur, """
            SELECT
                indexrelname as index_name,
                pg_size_pretty(pg_relation_size(indexrelid)) as size
            FROM pg_stat_user_indexes
            WHERE relname = 'products_json'
            AND indexrelname LIKE '%jsonb%'
        """, "JSONB 인덱스 크기 비교")

        # 2-4: 중첩 JSON 쿼리
        print_subsection("2-4: 중첩 JSON 쿼리")

        query_nested = """
            SELECT id, name,
                   attributes->'specs'->>'cpu' as cpu,
                   attributes->'specs'->>'ram' as ram
            FROM products_json
            WHERE attributes @> '{"specs": {"cpu": "i7"}}'
        """

        print("\n중첩 JSON 검색 (specs.cpu = 'i7'):")
        print(get_explain_analyze(cur, query_nested))
        execute_and_show(cur, query_nested)

        print("""
★ 핵심 정리:
  1. GIN은 JSONB 내부 검색에 필수
  2. @> (contains)가 가장 일반적인 연산자
  3. jsonb_path_ops는 @> 전용, 크기 작고 빠름
  4. 키 존재 검색(?)이 필요하면 기본 ops 사용
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 3: GIN 인덱스 - 배열과 전문검색
# =============================================================================

def scenario_3_gin_array_text():
    """
    시나리오 3: GIN 인덱스 - 배열과 전문검색

    GIN은 배열과 전문검색에도 활용됩니다.
    - 배열: @>, &&, <@ 연산자
    - 전문검색: pg_trgm, tsvector
    """
    print_section("시나리오 3: GIN 인덱스 - 배열과 전문검색")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # 3-1: 배열 인덱스
        print_subsection("3-1: 배열(Array) 검색")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ 배열 GIN 인덱스                                                  │
├─────────────────────────────────────────────────────────────────┤
│ tags TEXT[] 컬럼 예시:                                           │
│   Row1: ['electronics', 'computer', 'portable']                  │
│   Row2: ['electronics', 'accessory', 'wireless']                 │
│                                                                  │
│ GIN 인덱스:                                                      │
│   'electronics' → [Row1, Row2, ...]                              │
│   'computer'    → [Row1, ...]                                    │
│   'portable'    → [Row1, ...]                                    │
│                                                                  │
│ 연산자:                                                          │
│   @>  : 포함 (tags @> '{electronics}')                           │
│   &&  : 겹침 (tags && '{computer, gaming}')                      │
│   <@  : 포함됨 (tags <@ '{a, b, c}')                             │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 배열 포함 검색
        query_array = """
            SELECT id, name, tags
            FROM products_json
            WHERE tags @> ARRAY['electronics']
            LIMIT 5
        """

        print("\n@> 연산자: 'electronics' 태그 포함:")
        print(get_explain_analyze(cur, query_array))
        execute_and_show(cur, query_array)

        # 배열 겹침 검색
        query_overlap = """
            SELECT id, name, tags
            FROM products_json
            WHERE tags && ARRAY['gaming', 'portable']
            LIMIT 5
        """

        print("\n&& 연산자: 'gaming' 또는 'portable' 태그:")
        print(get_explain_analyze(cur, query_overlap))
        execute_and_show(cur, query_overlap)

        # 3-2: pg_trgm (유사 문자열 검색)
        print_subsection("3-2: pg_trgm - 유사 문자열 검색")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ pg_trgm (Trigram) 인덱스                                         │
├─────────────────────────────────────────────────────────────────┤
│ 문자열을 3글자(trigram) 단위로 분해하여 인덱싱                    │
│                                                                  │
│ 예: 'Laptop' → ['  l', ' la', 'lap', 'apt', 'pto', 'top', 'op '] │
│                                                                  │
│ 지원 연산자:                                                      │
│   LIKE '%keyword%'  : 부분 문자열 검색                           │
│   ILIKE             : 대소문자 무시                               │
│   ~, ~*             : 정규표현식                                 │
│   %, <->            : 유사도 검색                                 │
│                                                                  │
│ ★ B-tree로는 '%keyword%' 검색 불가! → pg_trgm 필요               │
└─────────────────────────────────────────────────────────────────┘
        """)

        # LIKE 검색 (pg_trgm 활용)
        query_like = """
            SELECT id, name
            FROM products_json
            WHERE name ILIKE '%pro%'
            LIMIT 5
        """

        print("\nILIKE '%pro%' 검색 (pg_trgm 인덱스 활용):")
        print(get_explain_analyze(cur, query_like))
        execute_and_show(cur, query_like)

        # 유사도 검색
        cur.execute("SET pg_trgm.similarity_threshold = 0.3")

        query_similar = """
            SELECT name, similarity(name, 'Laptop') as sim
            FROM products_json
            WHERE name % 'Laptop'
            ORDER BY sim DESC
            LIMIT 5
        """

        print("\n유사도 검색 (name % 'Laptop'):")
        execute_and_show(cur, query_similar)

        # 3-3: 전문검색 (Full-Text Search) 미리보기
        print_subsection("3-3: 전문검색 (Full-Text Search) 미리보기")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ tsvector / tsquery (전문검색)                                    │
├─────────────────────────────────────────────────────────────────┤
│ tsvector: 텍스트를 검색 가능한 벡터로 변환                        │
│   'High performance laptop' → 'high':1 'laptop':3 'perform':2    │
│                                                                  │
│ tsquery: 검색 쿼리 생성                                          │
│   to_tsquery('laptop & performance')                             │
│                                                                  │
│ 연산자:                                                          │
│   @@  : 매칭 (tsvector @@ tsquery)                               │
│                                                                  │
│ ★ 형태소 분석, 불용어 제거 등 고급 텍스트 검색 가능               │
└─────────────────────────────────────────────────────────────────┘
        """)

        query_fts = """
            SELECT id, name, description,
                   ts_rank(to_tsvector('english', description),
                           to_tsquery('english', 'laptop | gaming')) as rank
            FROM products_json
            WHERE to_tsvector('english', description) @@
                  to_tsquery('english', 'laptop | gaming')
            ORDER BY rank DESC
            LIMIT 5
        """

        print("\n전문검색: 'laptop' 또는 'gaming':")
        execute_and_show(cur, query_fts)

        print("""
★ 핵심 정리:
  1. 배열 검색: @> (포함), && (겹침) 연산자 + GIN
  2. 부분 문자열 검색: pg_trgm + GIN (LIKE '%x%' 가능!)
  3. 전문검색: tsvector/tsquery + GIN
  4. 각 용도에 맞는 GIN 인덱스 생성 필요
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 4: BRIN 인덱스
# =============================================================================

def scenario_4_brin():
    """
    시나리오 4: BRIN (Block Range Index)

    BRIN은 물리적으로 정렬된 대용량 데이터에 최적입니다.
    - 매우 작은 인덱스 크기 (B-tree의 1/100 이하)
    - 시계열 데이터, 로그 테이블에 적합
    """
    print_section("시나리오 4: BRIN 인덱스")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ BRIN (Block Range Index) 원리                                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ B-tree: 모든 행에 대한 인덱스 엔트리 생성                         │
│   → 정확하지만 크기가 큼                                         │
│                                                                  │
│ BRIN: 블록 범위(기본 128블록)마다 min/max만 저장                  │
│   Block 1-128:   min=2024-01-01, max=2024-01-10                  │
│   Block 129-256: min=2024-01-10, max=2024-01-20                  │
│   ...                                                            │
│                                                                  │
│ 검색 시:                                                         │
│   WHERE date = '2024-01-15'                                      │
│   → Block 129-256 범위만 스캔 (나머지 skip)                       │
│                                                                  │
│ ★ 조건: 데이터가 물리적으로 정렬되어 있어야 효과적!               │
│   (INSERT 순서 = 검색 기준 컬럼 순서)                             │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 4-1: 데이터 분포 확인
        print_subsection("4-1: sensor_data 테이블 데이터 분포")

        execute_and_show(cur, """
            SELECT
                COUNT(*) as total_rows,
                MIN(recorded_at) as min_date,
                MAX(recorded_at) as max_date
            FROM sensor_data
        """, "sensor_data 통계")

        # 4-2: B-tree vs BRIN 크기 비교
        print_subsection("4-2: B-tree vs BRIN 인덱스 크기 비교")

        # 비교용 B-tree 인덱스 생성
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sensor_recorded_btree
            ON sensor_data(recorded_at)
        """)
        conn.commit()

        execute_and_show(cur, """
            SELECT
                indexrelname as index_name,
                pg_size_pretty(pg_relation_size(indexrelid)) as size,
                CASE
                    WHEN indexrelname LIKE '%brin%' THEN 'BRIN'
                    ELSE 'B-tree'
                END as type
            FROM pg_stat_user_indexes
            WHERE relname = 'sensor_data'
            ORDER BY pg_relation_size(indexrelid)
        """, "인덱스 크기 비교")

        print("""
★ BRIN은 B-tree 대비 매우 작음!
  - 10만 건 기준 약 1/50 ~ 1/100 크기
  - 수억 건 테이블에서 더욱 효과적
        """)

        # 4-3: 쿼리 성능 비교
        print_subsection("4-3: 쿼리 성능 비교")

        # BRIN 사용
        query_brin = """
            SELECT COUNT(*), AVG(reading)
            FROM sensor_data
            WHERE recorded_at BETWEEN '2024-01-01' AND '2024-01-10'
        """

        # 힌트로 BRIN 사용 유도
        cur.execute("SET enable_indexscan = off")  # B-tree 비활성화
        print("\n[BRIN 인덱스 사용]")
        print(get_explain_analyze(cur, query_brin))

        cur.execute("SET enable_indexscan = on")
        cur.execute("SET enable_bitmapscan = off")  # BRIN(bitmap) 비활성화
        print("\n[B-tree 인덱스 사용]")
        print(get_explain_analyze(cur, query_brin))

        # 설정 복원
        cur.execute("SET enable_bitmapscan = on")

        execute_and_show(cur, query_brin, "쿼리 결과")

        # 4-4: BRIN이 적합하지 않은 경우
        print_subsection("4-4: BRIN이 적합하지 않은 경우")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ BRIN 사용 시 주의사항                                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ ✗ BRIN이 비효율적인 경우:                                        │
│   - 데이터가 물리적으로 랜덤하게 저장된 경우                      │
│   - 자주 UPDATE되어 순서가 깨진 경우                              │
│   - 높은 정확도가 필요한 OLTP 쿼리                                │
│                                                                  │
│ ✓ BRIN이 효과적인 경우:                                          │
│   - 시계열 데이터 (로그, 센서, 이벤트)                            │
│   - append-only 또는 INSERT만 하는 테이블                         │
│   - 대용량 테이블에서 범위 쿼리                                   │
│   - 인덱스 크기를 최소화해야 할 때                                │
│                                                                  │
│ ★ pages_per_range 파라미터로 정밀도 조절 가능                     │
│   - 작을수록: 정확도↑, 크기↑                                     │
│   - 클수록: 정확도↓, 크기↓                                        │
└─────────────────────────────────────────────────────────────────┘
        """)

        # sensor_id로 검색 (물리적 정렬 안됨 - 비효율적)
        query_random = """
            SELECT COUNT(*)
            FROM sensor_data
            WHERE sensor_id = 50
        """

        print("\nsensor_id 검색 (물리적 정렬 안됨):")
        print(get_explain_analyze(cur, query_random))
        print("→ BRIN 사용 안됨, Sequential Scan 발생!")

        print("""
★ 핵심 정리:
  1. BRIN = 매우 작은 크기, 물리적 정렬된 데이터에 최적
  2. 시계열/로그 테이블에 강력 추천
  3. 랜덤 데이터, 자주 UPDATE되는 테이블에는 비적합
  4. B-tree 대비 1/50~1/100 크기로 대용량 테이블에 필수
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 5: 인덱스 선택 가이드
# =============================================================================

def scenario_5_selection_guide():
    """
    시나리오 5: 인덱스 유형 선택 가이드

    쿼리 패턴과 데이터 특성에 따른 인덱스 선택 기준을 제시합니다.
    """
    print_section("시나리오 5: 인덱스 유형 선택 가이드")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────────────────┐
│                         인덱스 유형 선택 매트릭스                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  쿼리 패턴              │ B-tree │  GIN   │  BRIN  │  Hash  │    GiST       │
│  ───────────────────────┼────────┼────────┼────────┼────────┼───────────────│
│  = (등호)               │   ✓    │   -    │   △    │   ✓    │      -        │
│  <, >, BETWEEN (범위)   │   ✓    │   -    │   ✓    │   -    │      -        │
│  ORDER BY (정렬)        │   ✓    │   -    │   -    │   -    │      -        │
│  LIKE 'x%' (전방일치)   │   ✓    │   -    │   -    │   -    │      -        │
│  LIKE '%x%' (부분일치)  │   -    │  ✓*    │   -    │   -    │      -        │
│  JSONB @>, ?, ?|        │   -    │   ✓    │   -    │   -    │      -        │
│  배열 @>, &&            │   -    │   ✓    │   -    │   -    │      -        │
│  전문검색 @@            │   -    │   ✓    │   -    │   -    │      -        │
│  지리/기하 연산         │   -    │   -    │   -    │   -    │      ✓        │
│                                                                              │
│  ✓ = 최적, △ = 가능하나 제한적, - = 미지원                                  │
│  * pg_trgm 확장 필요                                                         │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                         데이터 특성별 선택 기준                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  데이터 특성                        │ 추천 인덱스                             │
│  ──────────────────────────────────┼────────────────────────────────────────│
│  일반 OLTP (등호, 범위, 정렬)       │ B-tree                                 │
│  JSONB 내부 검색                    │ GIN (jsonb_path_ops or 기본)           │
│  태그/카테고리 배열                 │ GIN                                    │
│  시계열/로그 (대용량, append-only)  │ BRIN                                   │
│  전문검색 (Full-Text Search)        │ GIN + tsvector                         │
│  유사 문자열 검색 (LIKE '%x%')      │ GIN + pg_trgm                          │
│  지리 정보 (PostGIS)               │ GiST                                    │
│  등호 검색만 (범위 없음)            │ Hash (PostgreSQL 10+)                  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
        """)

        # 현재 데이터베이스의 인덱스 현황
        print_subsection("현재 데이터베이스 인덱스 현황")

        execute_and_show(cur, """
            SELECT
                relname as table_name,
                indexrelname as index_name,
                CASE
                    WHEN indexdef LIKE '%USING btree%' THEN 'B-tree'
                    WHEN indexdef LIKE '%USING gin%' THEN 'GIN'
                    WHEN indexdef LIKE '%USING brin%' THEN 'BRIN'
                    WHEN indexdef LIKE '%USING hash%' THEN 'Hash'
                    WHEN indexdef LIKE '%USING gist%' THEN 'GiST'
                    ELSE 'B-tree'
                END as index_type,
                pg_size_pretty(pg_relation_size(indexrelid)) as size,
                idx_scan as scans
            FROM pg_stat_user_indexes
            JOIN pg_indexes ON indexrelname = indexname
            WHERE schemaname = 'public'
            ORDER BY relname, indexrelname
        """, "모든 인덱스 목록")

        # 의사결정 플로우차트
        print("""
┌─────────────────────────────────────────────────────────────────┐
│                   인덱스 선택 의사결정 흐름                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  [쿼리 분석 시작]                                                │
│        │                                                         │
│        ▼                                                         │
│  ┌─ JSONB/배열/전문검색? ─┐                                      │
│  │         │              │                                      │
│  │ Yes     │ No           │                                      │
│  ▼         │              │                                      │
│ GIN        │              │                                      │
│            ▼              │                                      │
│  ┌─ 대용량 + 물리정렬? ──┐                                       │
│  │         │             │                                       │
│  │ Yes     │ No          │                                       │
│  ▼         │             │                                       │
│ BRIN       │             │                                       │
│            ▼             │                                       │
│  ┌─ 범위/정렬 필요? ────┐                                        │
│  │         │            │                                        │
│  │ Yes     │ No         │                                        │
│  ▼         │            │                                        │
│ B-tree     ▼            │                                        │
│       (등호만?)          │                                        │
│        │   │            │                                        │
│   Yes  │   │ No         │                                        │
│    ▼   │   ▼            │                                        │
│  Hash  │ B-tree         │                                        │
│                                                                  │
│ ★ 대부분의 경우 B-tree가 기본 선택!                              │
│   특수한 경우에만 다른 인덱스 고려                                │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 실무 팁
        print("""
┌─────────────────────────────────────────────────────────────────┐
│                         실무 팁                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 1. 복합 인덱스 설계 시:                                          │
│    - 가장 자주 사용되는 컬럼을 앞에                               │
│    - 선택도(selectivity)가 높은 컬럼을 앞에                       │
│    - 범위 조건 컬럼은 마지막에                                    │
│                                                                  │
│ 2. 인덱스 과다 생성 주의:                                        │
│    - 쓰기 성능 저하 (INSERT/UPDATE마다 인덱스 갱신)               │
│    - 저장 공간 증가                                               │
│    - pg_stat_user_indexes로 사용률 모니터링                       │
│                                                                  │
│ 3. 주기적 유지보수:                                              │
│    - 미사용 인덱스 제거                                          │
│    - bloated 인덱스 REINDEX                                       │
│    - VACUUM ANALYZE 정기 실행                                     │
│                                                                  │
│ 4. 테스트 환경에서 먼저:                                         │
│    - 프로덕션과 유사한 데이터로 테스트                            │
│    - EXPLAIN ANALYZE로 실제 성능 확인                             │
│    - 인덱스 생성 시 CONCURRENTLY 옵션 사용                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        print("""
★ 최종 정리:
  1. B-tree: 기본 선택, 범위/정렬/등호 모두 지원
  2. GIN: JSONB, 배열, 전문검색에 필수
  3. BRIN: 시계열 대용량 테이블에 강력 추천
  4. 인덱스는 "필요한 만큼만" - 과다 생성 주의!
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 메인 실행
# =============================================================================

def main():
    print("""
╔══════════════════════════════════════════════════════════════════╗
║          Lab 08: 인덱스 유형과 활용                               ║
║          Index Types: B-tree, GIN, BRIN                          ║
╚══════════════════════════════════════════════════════════════════╝

이 실습에서는 PostgreSQL의 주요 인덱스 유형을 학습합니다.

시나리오 목록:
  1. B-tree 인덱스 심화 (범위, 복합, 정렬)
  2. GIN 인덱스 - JSONB (@>, ?, jsonb_path_ops)
  3. GIN 인덱스 - 배열과 전문검색
  4. BRIN 인덱스 (대용량 시계열 데이터)
  5. 인덱스 선택 가이드

실행할 시나리오 번호를 입력하세요 (1-5, 또는 'all'):
    """)

    scenarios = {
        '1': scenario_1_btree_advanced,
        '2': scenario_2_gin_jsonb,
        '3': scenario_3_gin_array_text,
        '4': scenario_4_brin,
        '5': scenario_5_selection_guide,
    }

    choice = input("선택: ").strip().lower()

    if choice == 'all':
        for num in sorted(scenarios.keys()):
            scenarios[num]()
            print("\n" + "─" * 70)
            input("다음 시나리오로 계속하려면 Enter를 누르세요...")
    elif choice in scenarios:
        scenarios[choice]()
    else:
        print("잘못된 선택입니다. 1-5 또는 'all'을 입력하세요.")


if __name__ == '__main__':
    main()
