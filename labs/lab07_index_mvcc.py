"""
Lab 07: 인덱스 기초와 MVCC
=========================

학습 목표:
- B-tree 인덱스가 MVCC와 어떻게 상호작용하는지 이해
- UPDATE 시 인덱스 포인터(ctid)의 변화 관찰
- HOT UPDATE가 인덱스 bloat을 방지하는 원리 심화
- pageinspect로 B-tree 인덱스 내부 구조 확인

MVCC 연결:
- Lab 02에서 배운 "UPDATE = DELETE + INSERT"가 인덱스에 미치는 영향
- 인덱스에는 xmin/xmax가 없음 → heap fetch로 가시성 확인 필수

실행 방법:
    python lab07_index_mvcc.py
"""

import psycopg2
from tabulate import tabulate

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'mvcc_lab',
    'user': 'study',
    'password': 'study123'
}


def get_connection(autocommit=False):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = autocommit
    return conn


def print_section(title):
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print('=' * 60)


def print_result(cursor, description=""):
    if description:
        print(f"\n>> {description}")
    rows = cursor.fetchall()
    if rows:
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt='psql'))
    else:
        print("(결과 없음)")
    return rows


def scenario_1_index_and_ctid():
    """
    시나리오 1: 인덱스와 ctid의 관계
    -------------------------------
    B-tree 인덱스는 (key, ctid) 쌍을 저장합니다.
    ctid는 튜플의 물리적 위치를 가리킵니다.
    """
    print_section("시나리오 1: 인덱스와 ctid의 관계")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    try:
        print("""
    B-tree 인덱스의 구조:
    ┌─────────────────────────────────────────────────────────┐
    │  인덱스 엔트리 = (indexed_value, ctid)                   │
    │                                                          │
    │  예: indexed_col = 42인 row가 (0, 5)에 있다면            │
    │      인덱스 엔트리: (42, "(0,5)")                         │
    │                                                          │
    │  핵심: 인덱스에는 xmin/xmax가 없음!                       │
    │        → 가시성 판단은 heap에서만 가능                    │
    └─────────────────────────────────────────────────────────┘
        """)

        # 테이블과 인덱스의 ctid 비교
        cur.execute("""
            SELECT
                t.ctid as heap_ctid,
                t.indexed_col,
                t.non_indexed_col
            FROM index_mvcc_test t
            WHERE t.indexed_col <= 5
            ORDER BY t.indexed_col
        """)
        print_result(cur, "테이블의 ctid와 데이터")

        # B-tree 인덱스 내부 확인 (bt_page_items)
        print("\n[B-tree 인덱스 내부 확인 - bt_page_items]")
        cur.execute("""
            SELECT itemoffset, ctid, data
            FROM bt_page_items('idx_mvcc_indexed', 1)
            LIMIT 10
        """)
        print_result(cur, "인덱스 리프 페이지의 엔트리들")

        print("""
    분석:
    - ctid: 인덱스가 가리키는 heap 튜플의 위치
    - data: 인덱스 키 값 (16진수 형태)

    인덱스 스캔 과정:
    1. 인덱스에서 조건에 맞는 ctid 찾기
    2. ctid로 heap 페이지 접근
    3. heap 튜플에서 xmin/xmax로 가시성 확인
    4. 가시성 통과 시 결과에 포함
        """)

    finally:
        cur.close()
        conn.close()


def scenario_2_update_indexed_column():
    """
    시나리오 2: 인덱스 컬럼 UPDATE 시 변화
    ------------------------------------
    인덱스 컬럼이 변경되면 인덱스에도 새 엔트리가 추가됩니다.
    """
    print_section("시나리오 2: 인덱스 컬럼 UPDATE 시 변화")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    # 테스트 테이블 생성
    cur.execute("""
        DROP TABLE IF EXISTS idx_update_test;
        CREATE TABLE idx_update_test (
            id SERIAL PRIMARY KEY,
            indexed_col INTEGER,
            non_indexed_col INTEGER
        );
        CREATE INDEX idx_test_indexed ON idx_update_test(indexed_col);
        INSERT INTO idx_update_test (indexed_col, non_indexed_col)
        VALUES (100, 1000);
    """)

    try:
        # UPDATE 전 상태
        print("\n[UPDATE 전]")
        cur.execute("""
            SELECT ctid, xmin, xmax, indexed_col, non_indexed_col
            FROM idx_update_test WHERE id = 1
        """)
        before = print_result(cur, "테이블 상태")
        old_ctid = before[0][0]

        cur.execute("""
            SELECT itemoffset, ctid, data
            FROM bt_page_items('idx_test_indexed', 1)
        """)
        print_result(cur, "인덱스 상태")

        # 인덱스 컬럼 UPDATE
        print("\n[indexed_col UPDATE: 100 → 200]")
        cur.execute("""
            UPDATE idx_update_test
            SET indexed_col = 200
            WHERE id = 1
        """)

        # UPDATE 후 상태
        print("\n[UPDATE 후]")
        cur.execute("""
            SELECT ctid, xmin, xmax, indexed_col, non_indexed_col
            FROM idx_update_test WHERE id = 1
        """)
        after = print_result(cur, "테이블 상태")
        new_ctid = after[0][0]

        cur.execute("""
            SELECT itemoffset, ctid, data
            FROM bt_page_items('idx_test_indexed', 1)
        """)
        print_result(cur, "인덱스 상태")

        # 인덱스 크기 확인
        cur.execute("""
            SELECT pg_size_pretty(pg_relation_size('idx_test_indexed')) as index_size
        """)
        print_result(cur, "인덱스 크기")

        print(f"""
    분석:
    - 이전 ctid: {old_ctid} → 새 ctid: {new_ctid}
    - 인덱스에 새 엔트리 추가됨 (200 → new_ctid)
    - 기존 인덱스 엔트리 (100 → old_ctid)는 dead 상태

    인덱스 컬럼 UPDATE의 비용:
    1. 기존 heap 튜플 삭제 표시 (xmax 설정)
    2. 새 heap 튜플 생성
    3. 기존 인덱스 엔트리 삭제 표시
    4. 새 인덱스 엔트리 추가
    → 인덱스 bloat의 원인!
        """)

    finally:
        cur.execute("DROP TABLE IF EXISTS idx_update_test")
        cur.close()
        conn.close()


def scenario_3_hot_update():
    """
    시나리오 3: HOT UPDATE (Heap-Only Tuple)
    ---------------------------------------
    인덱스 컬럼이 변경되지 않으면 인덱스 업데이트를 생략할 수 있습니다.
    """
    print_section("시나리오 3: HOT UPDATE (인덱스 bloat 방지)")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS hot_update_test;
        CREATE TABLE hot_update_test (
            id SERIAL PRIMARY KEY,
            indexed_col INTEGER,
            non_indexed_col INTEGER
        );
        CREATE INDEX idx_hot_indexed ON hot_update_test(indexed_col);
        INSERT INTO hot_update_test (indexed_col, non_indexed_col)
        VALUES (100, 1000);
    """)

    try:
        print("""
    HOT UPDATE란?
    - Heap-Only Tuple UPDATE
    - 인덱스 컬럼이 변경되지 않을 때 활성화
    - 인덱스 업데이트 생략 → 인덱스 bloat 방지

    조건:
    1. 인덱스 컬럼이 변경되지 않음
    2. 새 튜플이 같은 페이지에 들어갈 공간이 있음
        """)

        # UPDATE 전 인덱스 크기
        cur.execute("""
            SELECT pg_relation_size('idx_hot_indexed') as index_bytes
        """)
        before_size = cur.fetchone()[0]
        print(f"\n[UPDATE 전] 인덱스 크기: {before_size} bytes")

        # 비인덱스 컬럼 UPDATE (HOT 가능)
        print("\n[non_indexed_col UPDATE: 1000 → 2000 (인덱스 컬럼 불변)]")
        cur.execute("""
            UPDATE hot_update_test
            SET non_indexed_col = 2000
            WHERE id = 1
        """)

        # heap_page_items로 HOT 플래그 확인
        cur.execute("""
            SELECT
                lp as line_pointer,
                t_xmin, t_xmax, t_ctid,
                CASE WHEN (t_infomask2 & 16384) != 0 THEN 'HOT updated' ELSE '' END as hot_flag,
                CASE WHEN (t_infomask2 & 32768) != 0 THEN 'Heap-only' ELSE '' END as heap_only
            FROM heap_page_items(get_raw_page('hot_update_test', 0))
            WHERE t_data IS NOT NULL
        """)
        print_result(cur, "heap 튜플 상태 (HOT 플래그)")

        # UPDATE 후 인덱스 크기
        cur.execute("""
            SELECT pg_relation_size('idx_hot_indexed') as index_bytes
        """)
        after_size = cur.fetchone()[0]
        print(f"\n[UPDATE 후] 인덱스 크기: {after_size} bytes")

        # 인덱스 엔트리 수 확인
        cur.execute("""
            SELECT COUNT(*) as entry_count
            FROM bt_page_items('idx_hot_indexed', 1)
        """)
        print_result(cur, "인덱스 엔트리 수")

        print(f"""
    분석:
    - 인덱스 크기 변화: {before_size} → {after_size} bytes
    - 인덱스 엔트리 수: 변하지 않음!

    HOT UPDATE의 장점:
    1. 인덱스 쓰기 작업 생략 → 성능 향상
    2. 인덱스 bloat 방지 → 공간 절약
    3. VACUUM 부담 감소

    HOT chain:
    - 기존 튜플이 새 튜플을 가리킴 (t_ctid)
    - 인덱스는 여전히 기존 위치를 가리킴
    - 기존 위치 → HOT chain 따라가기 → 최신 버전
        """)

    finally:
        cur.execute("DROP TABLE IF EXISTS hot_update_test")
        cur.close()
        conn.close()


def scenario_4_index_visibility():
    """
    시나리오 4: 인덱스와 MVCC 가시성
    -------------------------------
    인덱스에는 xmin/xmax가 없으므로 heap fetch가 필수입니다.
    """
    print_section("시나리오 4: 인덱스와 MVCC 가시성")

    conn_t1 = get_connection()
    conn_t2 = get_connection()
    conn_monitor = get_connection(autocommit=True)

    cur_t1 = conn_t1.cursor()
    cur_t2 = conn_t2.cursor()
    cur_monitor = conn_monitor.cursor()

    # 테스트 테이블
    cur_monitor.execute("""
        DROP TABLE IF EXISTS visibility_test;
        CREATE TABLE visibility_test (
            id SERIAL PRIMARY KEY,
            value INTEGER
        );
        CREATE INDEX idx_visibility ON visibility_test(value);
        INSERT INTO visibility_test (value) VALUES (100);
    """)

    try:
        print("""
    핵심 개념:
    - 인덱스 엔트리에는 가시성 정보(xmin/xmax)가 없음!
    - 인덱스 스캔 후 반드시 heap fetch로 가시성 확인

    시나리오:
    T1: value = 100 삭제 (커밋 안함)
    T2: value = 100 검색 → 인덱스에서 찾지만 보여야 할까?
        """)

        # T1: 삭제 (커밋 안함)
        print("\n[T1] DELETE WHERE value = 100 (커밋 안함)")
        cur_t1.execute("BEGIN")
        cur_t1.execute("DELETE FROM visibility_test WHERE value = 100")

        # heap 상태 확인
        cur_monitor.execute("""
            SELECT xmin, xmax, ctid, value
            FROM visibility_test WHERE value = 100
        """)
        print_result(cur_monitor, "T1의 뷰 (삭제 후)")

        # T2: 검색
        print("\n[T2] SELECT * WHERE value = 100")
        cur_t2.execute("BEGIN")
        cur_t2.execute("SELECT xmin, xmax, ctid, value FROM visibility_test WHERE value = 100")
        result = print_result(cur_t2, "T2의 뷰")

        # EXPLAIN으로 확인
        cur_t2.execute("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM visibility_test WHERE value = 100")
        print("\n[T2] 실행 계획:")
        for row in cur_t2.fetchall():
            print(f"   {row[0]}")

        conn_t1.rollback()
        conn_t2.rollback()

        print("""
    분석:
    - T2는 아직 value = 100 row를 볼 수 있음
    - 이유: T1이 커밋하지 않았으므로 xmax가 "invisible"

    인덱스 스캔 + 가시성 확인 과정:
    1. 인덱스에서 value = 100인 ctid 찾기
    2. heap 페이지 접근
    3. 튜플의 xmin/xmax를 스냅샷으로 검사
    4. 가시성 통과 → 결과에 포함

    만약 T1이 커밋했다면?
    → T2는 해당 row를 보지 못함 (xmax가 커밋됨)
        """)

    finally:
        cur_monitor.execute("DROP TABLE IF EXISTS visibility_test")
        cur_t1.close()
        cur_t2.close()
        cur_monitor.close()
        conn_t1.close()
        conn_t2.close()
        conn_monitor.close()


def scenario_5_btree_structure():
    """
    시나리오 5: B-tree 인덱스 구조 확인
    ----------------------------------
    pageinspect로 B-tree의 메타데이터와 페이지 구조를 확인합니다.
    """
    print_section("시나리오 5: B-tree 인덱스 구조 확인")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    try:
        print("""
    B-tree 인덱스의 구조:
    ┌───────────────────────────────────────┐
    │            Meta Page (0)              │
    │  - root page 위치                      │
    │  - tree level (높이)                   │
    └───────────────────────────────────────┘
                     │
                     ▼
    ┌───────────────────────────────────────┐
    │         Root/Internal Pages           │
    │  - 하위 페이지로의 포인터              │
    └───────────────────────────────────────┘
                     │
                     ▼
    ┌───────────────────────────────────────┐
    │            Leaf Pages                 │
    │  - (key, ctid) 쌍 저장                │
    │  - 실제 데이터 위치 정보               │
    └───────────────────────────────────────┘
        """)

        # 인덱스 메타데이터
        cur.execute("""
            SELECT *
            FROM bt_metap('idx_mvcc_indexed')
        """)
        print_result(cur, "B-tree 메타데이터")

        # 특정 페이지 통계
        cur.execute("""
            SELECT *
            FROM bt_page_stats('idx_mvcc_indexed', 1)
        """)
        print_result(cur, "페이지 1 (첫 번째 리프) 통계")

        # 리프 페이지의 실제 엔트리들
        cur.execute("""
            SELECT itemoffset, ctid, itemlen, data
            FROM bt_page_items('idx_mvcc_indexed', 1)
            LIMIT 10
        """)
        print_result(cur, "리프 페이지 엔트리들 (상위 10개)")

        # 인덱스 크기 정보
        cur.execute("""
            SELECT
                pg_size_pretty(pg_relation_size('idx_mvcc_indexed')) as index_size,
                (SELECT count(*) FROM index_mvcc_test) as row_count
        """)
        print_result(cur, "인덱스 크기 vs 테이블 row 수")

        print("""
    주요 필드 설명:
    - magic: B-tree 인덱스 식별자
    - level: 트리 높이 (0 = 리프)
    - fastroot: 현재 root 페이지
    - live_items: 살아있는 인덱스 엔트리 수
    - dead_items: dead 인덱스 엔트리 수 (VACUUM 대상)
        """)

    finally:
        cur.close()
        conn.close()


def main():
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║           Lab 07: 인덱스 기초와 MVCC                        ║
    ║                                                           ║
    ║  B-tree 인덱스가 MVCC와 어떻게 상호작용하는지 학습합니다.     ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    try:
        scenario_1_index_and_ctid()
        scenario_2_update_indexed_column()
        scenario_3_hot_update()
        scenario_4_index_visibility()
        scenario_5_btree_structure()

        print_section("Lab 07 완료!")
        print("""
    학습 정리:

    1. 인덱스와 ctid:
       - 인덱스는 (key, ctid) 쌍을 저장
       - ctid로 heap 튜플 위치 참조

    2. 인덱스에는 가시성 정보가 없다:
       - xmin/xmax는 heap 튜플에만 존재
       - 인덱스 스캔 후 반드시 heap fetch 필요
       - 예외: Index-Only Scan (Lab 09에서 다룸)

    3. UPDATE 시 인덱스 변화:
       - 인덱스 컬럼 변경 → 인덱스 엔트리 추가 (bloat 원인)
       - 비인덱스 컬럼 변경 → HOT UPDATE 가능 (bloat 방지)

    4. HOT UPDATE:
       - 인덱스 업데이트 생략으로 성능 향상
       - 인덱스 bloat 방지
       - HOT chain으로 최신 버전 추적

    MVCC 연결 (Lab 02 복습):
    - UPDATE = 기존 튜플 삭제 표시 + 새 튜플 생성
    - 인덱스도 같은 패턴: 기존 엔트리 dead + 새 엔트리 추가

    다음 실습: lab08_index_types.py
        """)

    except psycopg2.OperationalError as e:
        print(f"\n오류: 데이터베이스에 연결할 수 없습니다.")
        print(f"Docker가 실행 중인지 확인하세요: docker-compose up -d")
        print(f"상세 오류: {e}")


if __name__ == "__main__":
    main()
