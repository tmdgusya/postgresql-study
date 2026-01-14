#!/usr/bin/env python3
"""
Lab 09: 쿼리 실행 계획과 최적화
============================

학습 목표:
- EXPLAIN ANALYZE 출력 완전 해석
- Index-Only Scan과 Visibility Map의 관계 이해 ★핵심!
- Covering Index, Partial Index 활용

★ 핵심 연결: Lab 02b (Snapshot) → Lab 09 (Visibility Map)
  - Snapshot: 트랜잭션이 어떤 튜플을 볼 수 있는지 결정
  - Visibility Map: 페이지 내 모든 튜플이 "모든 트랜잭션에 visible"인지 기록
  - all-visible 페이지는 Snapshot 확인 없이 바로 읽기 가능!

선수 지식: Lab 02b (Snapshot), Lab 07, Lab 08

사용 테이블:
- orders: 10만 건 주문 데이터 (Index-Only Scan 실습)
- index_mvcc_test: 1000건 기본 데이터
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


def get_explain_analyze(cur, query, buffers=True):
    """EXPLAIN ANALYZE 결과 반환"""
    options = "ANALYZE, COSTS, BUFFERS" if buffers else "ANALYZE, COSTS"
    cur.execute(f"EXPLAIN ({options}, FORMAT TEXT) {query}")
    return '\n'.join([row[0] for row in cur.fetchall()])


# =============================================================================
# 시나리오 1: EXPLAIN ANALYZE 해석
# =============================================================================

def scenario_1_explain_analyze():
    """
    시나리오 1: EXPLAIN ANALYZE 완전 해석

    실행 계획의 각 필드를 상세히 분석합니다.
    - cost, rows, actual time, buffers
    """
    print_section("시나리오 1: EXPLAIN ANALYZE 완전 해석")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ EXPLAIN ANALYZE 출력 구조                                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ Seq Scan on orders  (cost=0.00..1834.00 rows=100000 width=44)   │
│                      ─────────────── ──────── ────────          │
│                      startup..total  예상행수 행크기(bytes)      │
│                                                                  │
│   (actual time=0.012..15.234 rows=100000 loops=1)               │
│    ──────────────────────── ──────── ─────                      │
│    시작..종료 시간(ms)       실제행수 반복횟수                    │
│                                                                  │
│ Buffers: shared hit=834 read=200                                │
│          ──────────── ────────                                  │
│          캐시 히트    디스크 읽기                                 │
│                                                                  │
│ Planning Time: 0.123 ms                                          │
│ Execution Time: 15.456 ms                                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 1-1: 기본 Seq Scan
        print_subsection("1-1: Sequential Scan (Seq Scan)")

        query_seq = "SELECT COUNT(*) FROM orders"
        print("\n전체 테이블 스캔:")
        print(get_explain_analyze(cur, query_seq))

        print("""
해석:
  - cost=0.00..1834.00: 시작 비용 0, 총 비용 1834
  - rows=100000: 예상 행 수 (실제와 비교!)
  - Buffers: shared hit = 캐시에서 읽음, read = 디스크에서 읽음
  - hit 비율이 높을수록 좋음 (캐시 효율)
        """)

        # 1-2: Index Scan
        print_subsection("1-2: Index Scan")

        query_idx = """
            SELECT id, customer_id, total_amount
            FROM orders
            WHERE customer_id = 100
        """
        print("\n인덱스 스캔:")
        print(get_explain_analyze(cur, query_idx))

        print("""
해석:
  - Index Scan using idx_orders_covering: 인덱스 사용
  - Index Cond: 인덱스로 필터링한 조건
  - Filter: 인덱스 외 추가 필터 (있으면 비효율 가능)
        """)

        # 1-3: Cost 계산 이해
        print_subsection("1-3: Cost 계산 원리")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ Cost 계산 방식                                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ cost = startup_cost..total_cost                                  │
│                                                                  │
│ startup_cost: 첫 번째 행을 반환하기까지의 비용                    │
│   - 정렬이 필요하면 startup_cost가 높음 (정렬 완료 후 반환)       │
│   - 인덱스 스캔은 startup_cost가 낮음 (바로 반환 가능)            │
│                                                                  │
│ total_cost: 모든 행을 반환하는 총 비용                           │
│   - 실제 시간이 아닌 상대적 비용 단위                             │
│                                                                  │
│ ★ 비용 파라미터 (postgresql.conf):                               │
│   seq_page_cost = 1.0        # 순차 페이지 읽기 기준              │
│   random_page_cost = 4.0     # 랜덤 I/O (인덱스 접근)            │
│   cpu_tuple_cost = 0.01      # 튜플 처리                         │
│   cpu_index_tuple_cost = 0.005                                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # Sort 연산 보기
        query_sort = """
            SELECT id, order_date, total_amount
            FROM orders
            WHERE customer_id = 100
            ORDER BY order_date DESC
        """
        print("\n정렬이 포함된 쿼리:")
        print(get_explain_analyze(cur, query_sort))

        print("""
★ 핵심 정리:
  1. cost는 상대적 비용, actual time은 실제 시간
  2. rows (예상) vs rows (실제) 차이가 크면 통계 갱신 필요
  3. Buffers hit이 높을수록 캐시 효율 좋음
  4. startup_cost가 높은 연산은 첫 행 반환이 느림
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 2: 스캔 방식 비교
# =============================================================================

def scenario_2_scan_types():
    """
    시나리오 2: 스캔 방식 비교

    Sequential Scan, Index Scan, Bitmap Scan의 차이를 이해합니다.
    플래너가 어떤 기준으로 스캔 방식을 선택하는지 학습합니다.
    """
    print_section("시나리오 2: 스캔 방식 비교")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ 스캔 방식 비교                                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 1. Sequential Scan (Seq Scan)                                    │
│    - 테이블 전체를 순차적으로 읽음                                │
│    - 대부분의 행이 필요할 때 효율적                               │
│    - 랜덤 I/O 없음 (디스크 순차 읽기)                            │
│                                                                  │
│ 2. Index Scan                                                    │
│    - 인덱스로 조건 만족하는 위치 찾음 → heap 접근                 │
│    - 소수의 행을 찾을 때 효율적                                   │
│    - 행마다 랜덤 I/O 발생 가능                                   │
│                                                                  │
│ 3. Bitmap Index Scan + Bitmap Heap Scan                         │
│    - 인덱스로 조건 만족하는 페이지 비트맵 생성                     │
│    - 비트맵 기준 heap 페이지 순차 접근                            │
│    - 중간 규모의 결과에 효율적                                    │
│                                                                  │
│ ★ 선택 기준: selectivity (선택도)                                │
│    - 낮은 선택도 (적은 행): Index Scan                           │
│    - 중간 선택도: Bitmap Scan                                    │
│    - 높은 선택도 (많은 행): Seq Scan                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 2-1: 선택도에 따른 스캔 방식 변화
        print_subsection("2-1: 선택도(Selectivity)에 따른 스캔 방식")

        # customer_id 분포 확인
        execute_and_show(cur, """
            SELECT
                MIN(customer_id), MAX(customer_id),
                COUNT(DISTINCT customer_id) as unique_customers
            FROM orders
        """, "customer_id 분포")

        # 매우 낮은 선택도 (단일 고객)
        print("\n[낮은 선택도] customer_id = 100 (약 10건):")
        print(get_explain_analyze(cur, """
            SELECT * FROM orders WHERE customer_id = 100
        """))

        # 중간 선택도
        print("\n[중간 선택도] customer_id BETWEEN 100 AND 500 (약 4000건):")
        print(get_explain_analyze(cur, """
            SELECT * FROM orders WHERE customer_id BETWEEN 100 AND 500
        """))

        # 높은 선택도
        print("\n[높은 선택도] customer_id < 5000 (약 50%):")
        print(get_explain_analyze(cur, """
            SELECT * FROM orders WHERE customer_id < 5000
        """))

        # 2-2: Bitmap Scan 상세
        print_subsection("2-2: Bitmap Scan 동작 원리")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ Bitmap Scan 동작 과정                                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 1. Bitmap Index Scan                                             │
│    - 인덱스를 스캔하여 조건 만족하는 행의 위치(ctid) 수집         │
│    - 페이지 단위로 비트맵 생성: [0,0,1,0,1,1,0,...]              │
│      (1 = 해당 페이지에 매칭 행 있음)                             │
│                                                                  │
│ 2. Bitmap Heap Scan                                              │
│    - 비트맵을 순서대로 스캔                                       │
│    - 표시된 페이지만 heap에서 읽음                                │
│    - Recheck Cond: 정확한 행 필터링 (lossy인 경우)               │
│                                                                  │
│ ★ 장점:                                                          │
│    - Index Scan보다 랜덤 I/O 감소 (페이지 단위 접근)              │
│    - 여러 인덱스 조건을 BitmapAnd/BitmapOr로 결합 가능            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # Bitmap 연산 예시 (BitmapOr)
        print("\nBitmapOr (OR 조건에서 여러 인덱스 결합):")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_status
            ON orders(status)
        """)
        conn.commit()

        print(get_explain_analyze(cur, """
            SELECT COUNT(*)
            FROM orders
            WHERE customer_id = 100 OR status = 'pending'
        """))

        print("""
★ 핵심 정리:
  1. 플래너는 selectivity 기반으로 스캔 방식 자동 선택
  2. Bitmap Scan은 중간 규모 결과에 최적
  3. 통계가 정확해야 올바른 선택 (ANALYZE 중요!)
  4. enable_seqscan, enable_indexscan 등으로 강제 가능 (테스트용)
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 3: Index-Only Scan과 Visibility Map ★핵심!
# =============================================================================

def scenario_3_index_only_scan():
    """
    시나리오 3: Index-Only Scan과 Visibility Map ★핵심!

    Lab 02b의 Snapshot 개념이 실제로 어떻게 최적화에 활용되는지 학습합니다.

    핵심 연결:
    - Snapshot: 트랜잭션의 가시성 판단 기준
    - Visibility Map: 페이지가 "all-visible"인지 기록
    - Index-Only Scan: all-visible 페이지는 heap 접근 생략!
    """
    print_section("시나리오 3: Index-Only Scan과 Visibility Map ★핵심!")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ ★ Lab 02b → Lab 09 핵심 연결 ★                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ [Lab 02b에서 배운 것]                                            │
│   Snapshot = (xmin, xmax, xip[])                                 │
│   → 트랜잭션이 어떤 튜플을 볼 수 있는지 결정                      │
│   → 인덱스에는 xmin/xmax가 없음 → heap 확인 필수!                 │
│                                                                  │
│ [그런데 문제]                                                     │
│   인덱스에서 찾은 모든 행에 대해 heap 접근?                       │
│   → 너무 비효율적!                                               │
│                                                                  │
│ [해결책: Visibility Map]                                         │
│   - 각 heap 페이지마다 1비트 플래그                              │
│   - "이 페이지의 모든 튜플이 모든 트랜잭션에 visible"             │
│   - all-visible = 1이면 → Snapshot 확인 불필요!                  │
│   - VACUUM이 dead tuple 정리 후 all-visible 표시                 │
│                                                                  │
│ [Index-Only Scan]                                                │
│   1. 필요한 모든 컬럼이 인덱스에 있음 (Covering Index)           │
│   2. Visibility Map에서 all-visible 확인                         │
│   3. all-visible이면 인덱스에서 바로 값 반환 (heap 생략!)        │
│   4. all-visible 아니면 heap 확인 (Heap Fetches 발생)            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 3-1: Visibility Map 상태 확인
        print_subsection("3-1: Visibility Map 상태 확인")

        # VACUUM으로 all-visible 상태 만들기
        cur.execute("VACUUM ANALYZE orders")
        conn.commit()

        execute_and_show(cur, """
            SELECT
                c.relname as table_name,
                pg_relation_size(c.oid) as table_size,
                pg_relation_size(c.reltoastrelid) as toast_size,
                COALESCE(
                    (SELECT SUM(all_visible::int)
                     FROM pg_visibility_map_summary(c.oid)),
                    0
                ) as all_visible_pages
            FROM pg_class c
            WHERE c.relname = 'orders'
        """, "orders 테이블 Visibility Map 상태")

        # pg_visibility 확장 사용 (있으면)
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_visibility")
            conn.commit()

            execute_and_show(cur, """
                SELECT
                    all_visible,
                    all_frozen,
                    pd_all_visible
                FROM pg_visibility('orders')
                LIMIT 10
            """, "처음 10개 페이지의 visibility 상태")
        except:
            print("\n(pg_visibility 확장 없음 - 생략)")

        # 3-2: Index-Only Scan 실제 동작
        print_subsection("3-2: Index-Only Scan 실제 동작")

        # Covering Index 확인
        execute_and_show(cur, """
            SELECT indexdef
            FROM pg_indexes
            WHERE indexname = 'idx_orders_covering'
        """, "Covering Index 정의")

        print("""
idx_orders_covering: (customer_id) INCLUDE (total_amount, status)
  - customer_id로 검색
  - total_amount, status는 인덱스에 포함 (INCLUDE)
  - 이 3개 컬럼만 SELECT하면 Index-Only Scan 가능!
        """)

        # Index-Only Scan 유도
        query_ios = """
            SELECT customer_id, total_amount, status
            FROM orders
            WHERE customer_id BETWEEN 100 AND 200
        """

        print("\nIndex-Only Scan 실행 계획:")
        explain = get_explain_analyze(cur, query_ios)
        print(explain)

        # Heap Fetches 분석
        if "Heap Fetches:" in explain:
            print("""
★ Heap Fetches 해석:
  - Heap Fetches: 0  → 모든 페이지가 all-visible, 완벽한 Index-Only Scan!
  - Heap Fetches: N  → N개 행에서 heap 확인 필요 (recent UPDATE 때문)

  Heap Fetches가 높으면:
  1. 최근 UPDATE/DELETE된 행이 많음
  2. VACUUM이 아직 안 돌았음
  → VACUUM 실행 후 재확인!
            """)

        # 3-3: UPDATE 후 Index-Only Scan 변화
        print_subsection("3-3: UPDATE 후 Index-Only Scan 변화")

        # 일부 행 UPDATE
        cur.execute("""
            UPDATE orders
            SET status = 'updated'
            WHERE customer_id = 150
        """)
        conn.commit()

        print("\n[UPDATE 직후] Index-Only Scan:")
        explain_after = get_explain_analyze(cur, query_ios)
        print(explain_after)

        print("""
★ UPDATE 후 변화:
  - UPDATE된 페이지의 all-visible 플래그가 0으로 변경
  - 해당 페이지 접근 시 Heap Fetches 발생
  - VACUUM 실행 전까지 유지
        """)

        # VACUUM 후 복구
        cur.execute("VACUUM orders")
        conn.commit()

        print("\n[VACUUM 후] Index-Only Scan:")
        print(get_explain_analyze(cur, query_ios))

        print("""
★ 핵심 정리:
  1. Visibility Map = 페이지별 "all-visible" 플래그
  2. all-visible 페이지는 Snapshot 확인 없이 읽기 가능
  3. Index-Only Scan = Covering Index + all-visible → heap 생략
  4. Heap Fetches = all-visible이 아닌 페이지에서의 heap 접근
  5. VACUUM이 dead tuple 정리 후 all-visible 표시
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 4: Covering Index
# =============================================================================

def scenario_4_covering_index():
    """
    시나리오 4: Covering Index (INCLUDE 절)

    Index-Only Scan을 가능하게 하는 Covering Index 설계를 학습합니다.
    """
    print_section("시나리오 4: Covering Index (INCLUDE 절)")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ Covering Index 설계                                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ [일반 인덱스]                                                     │
│   CREATE INDEX idx ON orders(customer_id)                        │
│   → customer_id만 인덱스에 저장                                  │
│   → 다른 컬럼 필요 시 heap 접근 필수                             │
│                                                                  │
│ [Covering Index - INCLUDE 절]                                    │
│   CREATE INDEX idx ON orders(customer_id)                        │
│     INCLUDE (total_amount, status)                               │
│                                                                  │
│   → customer_id: 검색 키 (B-tree 정렬됨)                         │
│   → total_amount, status: 페이로드 (정렬 안됨, 저장만)           │
│                                                                  │
│ ★ 차이점:                                                        │
│   - 검색 키: WHERE, ORDER BY에 사용 가능                         │
│   - INCLUDE: SELECT에만 사용 (검색/정렬 불가)                    │
│   - INCLUDE는 인덱스 크기를 적게 증가시킴                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 4-1: INCLUDE vs 복합 인덱스 비교
        print_subsection("4-1: INCLUDE vs 복합 인덱스 크기 비교")

        # 비교용 인덱스 생성
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_composite_full
            ON orders(customer_id, total_amount, status)
        """)
        conn.commit()

        execute_and_show(cur, """
            SELECT
                indexrelname as index_name,
                pg_size_pretty(pg_relation_size(indexrelid)) as size
            FROM pg_stat_user_indexes
            WHERE relname = 'orders'
            AND indexrelname IN ('idx_orders_covering', 'idx_orders_composite_full')
        """, "인덱스 크기 비교")

        print("""
★ Covering Index가 더 작을 수 있는 이유:
  - 복합 인덱스: 모든 컬럼으로 정렬 → 내부 노드에도 값 저장
  - INCLUDE: 리프 노드에만 페이로드 저장
        """)

        # 4-2: Covering Index 설계 가이드
        print_subsection("4-2: Covering Index 설계 가이드")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ Covering Index 설계 체크리스트                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 1. 쿼리 분석                                                     │
│    - WHERE 조건 컬럼 → 검색 키                                   │
│    - SELECT 컬럼 → INCLUDE 후보                                  │
│    - ORDER BY 컬럼 → 검색 키 (정렬 필요 시)                      │
│                                                                  │
│ 2. 검색 키 선정                                                   │
│    - 자주 검색하는 조건 컬럼                                      │
│    - 선택도가 높은(distinct values 많은) 컬럼                     │
│    - 범위 검색 컬럼은 마지막에                                    │
│                                                                  │
│ 3. INCLUDE 컬럼 선정                                              │
│    - SELECT에 자주 포함되는 컬럼                                  │
│    - 검색/정렬에 사용되지 않는 컬럼                               │
│    - 크기가 작은 컬럼 (TEXT/JSONB 주의)                          │
│                                                                  │
│ 4. 트레이드오프 고려                                              │
│    - 인덱스 크기 증가 vs heap fetch 감소                         │
│    - INSERT/UPDATE 성능 vs SELECT 성능                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 실제 예시
        print("\n[예시] 주문 조회 쿼리 최적화:")

        # 원래 쿼리
        query_original = """
            SELECT customer_id, order_date, total_amount
            FROM orders
            WHERE customer_id = 500
            AND status = 'confirmed'
        """

        print("\n기존 인덱스로 실행:")
        print(get_explain_analyze(cur, query_original))

        # 최적화된 인덱스
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_optimized
            ON orders(customer_id, status) INCLUDE (order_date, total_amount)
        """)
        conn.commit()

        cur.execute("ANALYZE orders")
        conn.commit()

        print("\n최적화된 Covering Index로 실행:")
        print(get_explain_analyze(cur, query_original))

        print("""
★ 핵심 정리:
  1. INCLUDE = SELECT용 페이로드 (검색/정렬 불가)
  2. 자주 함께 조회되는 컬럼을 INCLUDE
  3. 인덱스 크기 vs 성능 트레이드오프 고려
  4. pg_stat_user_indexes로 활용도 모니터링
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 5: Partial Index
# =============================================================================

def scenario_5_partial_index():
    """
    시나리오 5: Partial Index (부분 인덱스)

    특정 조건만 인덱싱하여 크기와 성능을 최적화합니다.
    """
    print_section("시나리오 5: Partial Index (부분 인덱스)")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ Partial Index 개념                                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 일반 인덱스:                                                     │
│   CREATE INDEX idx ON orders(order_date)                         │
│   → 모든 행 인덱싱 (10만 건)                                     │
│                                                                  │
│ Partial Index:                                                   │
│   CREATE INDEX idx ON orders(order_date)                         │
│     WHERE status = 'pending'                                     │
│   → 조건 만족하는 행만 인덱싱 (약 2만 건)                        │
│                                                                  │
│ ★ 장점:                                                          │
│   - 인덱스 크기 대폭 감소                                        │
│   - INSERT/UPDATE 오버헤드 감소                                  │
│   - 자주 조회하는 subset에 대한 빠른 검색                        │
│                                                                  │
│ ★ 활용 예:                                                       │
│   - 활성 상태 행만 (status = 'active')                           │
│   - 최근 데이터만 (created_at > NOW() - INTERVAL '30 days')      │
│   - 특정 카테고리만 (category = 'important')                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 5-1: Partial Index 생성 및 크기 비교
        print_subsection("5-1: Partial Index 크기 비교")

        # 전체 인덱스
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_date_full
            ON orders(order_date)
        """)

        # Partial Index (이미 init.sql에서 생성됨)
        # idx_orders_pending: WHERE status = 'pending'

        conn.commit()

        execute_and_show(cur, """
            SELECT
                indexrelname as index_name,
                pg_size_pretty(pg_relation_size(indexrelid)) as size,
                idx_scan as scans
            FROM pg_stat_user_indexes
            WHERE relname = 'orders'
            AND indexrelname LIKE '%order%date%' OR indexrelname LIKE '%pending%'
            ORDER BY indexrelname
        """, "order_date 관련 인덱스 크기 비교")

        # 상태별 분포 확인
        execute_and_show(cur, """
            SELECT status, COUNT(*) as count,
                   ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER() * 100, 1) as pct
            FROM orders
            GROUP BY status
            ORDER BY count DESC
        """, "orders 상태별 분포")

        # 5-2: Partial Index 사용 조건
        print_subsection("5-2: Partial Index 사용 조건")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ Partial Index 사용 조건                                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 쿼리의 WHERE 조건이 인덱스 조건을 "포함"해야 함                  │
│                                                                  │
│ 인덱스: WHERE status = 'pending'                                 │
│                                                                  │
│ ✓ 사용됨:                                                        │
│   WHERE status = 'pending' AND order_date > '2024-01-01'         │
│   WHERE status = 'pending'                                       │
│                                                                  │
│ ✗ 사용 안됨:                                                     │
│   WHERE status = 'confirmed'          (조건 불일치)              │
│   WHERE order_date > '2024-01-01'     (status 조건 없음)         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # Partial Index 활용 쿼리
        query_partial = """
            SELECT id, order_date, total_amount
            FROM orders
            WHERE status = 'pending'
            AND order_date > CURRENT_DATE - 30
        """

        print("\nPartial Index 사용 쿼리:")
        print(get_explain_analyze(cur, query_partial))

        # 조건 불일치 쿼리
        query_no_partial = """
            SELECT id, order_date, total_amount
            FROM orders
            WHERE status = 'confirmed'
            AND order_date > CURRENT_DATE - 30
        """

        print("\n조건 불일치 (Partial Index 미사용):")
        print(get_explain_analyze(cur, query_no_partial))

        # 5-3: Unique Partial Index
        print_subsection("5-3: Unique Partial Index")

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ Unique Partial Index - 조건부 유니크 제약                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ CREATE UNIQUE INDEX idx ON users(email)                          │
│   WHERE deleted_at IS NULL                                       │
│                                                                  │
│ → 삭제되지 않은 사용자 중에서만 email 유니크!                     │
│ → soft delete 패턴에서 유용                                      │
│                                                                  │
│ 예시 상황:                                                       │
│   user1: email='test@a.com', deleted_at=NULL     ← 유니크       │
│   user2: email='test@a.com', deleted_at='2024'   ← 중복 허용    │
│   user3: email='test@a.com', deleted_at=NULL     ← 에러!        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        print("""
★ 핵심 정리:
  1. Partial Index = WHERE 조건으로 인덱싱 범위 제한
  2. 자주 조회하는 subset에 대해 크기/성능 최적화
  3. 쿼리 조건이 인덱스 조건을 포함해야 사용됨
  4. Unique Partial Index로 조건부 유니크 제약 가능
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
║          Lab 09: 쿼리 실행 계획과 최적화                          ║
║          Query Plans and Optimization                            ║
╚══════════════════════════════════════════════════════════════════╝

이 실습에서는 쿼리 실행 계획을 분석하고 최적화 기법을 학습합니다.

★ 핵심 연결: Lab 02b (Snapshot) → Lab 09 (Visibility Map)

시나리오 목록:
  1. EXPLAIN ANALYZE 완전 해석
  2. 스캔 방식 비교 (Seq/Index/Bitmap)
  3. Index-Only Scan과 Visibility Map ★핵심!
  4. Covering Index (INCLUDE 절)
  5. Partial Index (부분 인덱스)

실행할 시나리오 번호를 입력하세요 (1-5, 또는 'all'):
    """)

    scenarios = {
        '1': scenario_1_explain_analyze,
        '2': scenario_2_scan_types,
        '3': scenario_3_index_only_scan,
        '4': scenario_4_covering_index,
        '5': scenario_5_partial_index,
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
