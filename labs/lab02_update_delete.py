"""
Lab 02: UPDATE/DELETE 시 튜플 변화 실습
=======================================

학습 목표:
- UPDATE가 내부적으로 DELETE + INSERT임을 확인
- UPDATE/DELETE 전후 xmin, xmax, ctid 변화 관찰
- pageinspect로 raw 튜플 데이터 직접 확인

실행 방법:
    python lab02_update_delete.py
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


def scenario_1_update_creates_new_tuple():
    """
    시나리오 1: UPDATE는 새 튜플을 생성한다
    ---------------------------------------
    UPDATE 전후의 xmin, xmax, ctid 변화를 관찰합니다.
    """
    print_section("시나리오 1: UPDATE는 새 튜플을 생성한다")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    # 테스트용 데이터 준비
    cur.execute("""
        INSERT INTO accounts (name, balance)
        VALUES ('Update Test User', 1000)
        RETURNING id
    """)
    test_id = cur.fetchone()[0]

    try:
        # UPDATE 전 상태 확인
        print("\n[UPDATE 전]")
        cur.execute("""
            SELECT xmin, xmax, ctid, id, name, balance
            FROM accounts WHERE id = %s
        """, (test_id,))
        before = print_result(cur, "UPDATE 전 튜플 상태")
        old_xmin = before[0][0]
        old_ctid = before[0][2]

        # UPDATE 실행
        print("\n[UPDATE 실행]")
        cur.execute("SELECT txid_current()")
        update_txid = cur.fetchone()[0]
        print(f"UPDATE를 실행하는 트랜잭션 ID: {update_txid}")

        cur.execute("""
            UPDATE accounts
            SET balance = 2000
            WHERE id = %s
        """, (test_id,))
        print(f"UPDATE 완료: balance를 1000 → 2000으로 변경")

        # UPDATE 후 상태 확인
        print("\n[UPDATE 후]")
        cur.execute("""
            SELECT xmin, xmax, ctid, id, name, balance
            FROM accounts WHERE id = %s
        """, (test_id,))
        after = print_result(cur, "UPDATE 후 튜플 상태")
        new_xmin = after[0][0]
        new_ctid = after[0][2]

        print(f"""
    분석:
    - 이전 xmin: {old_xmin} → 새 xmin: {new_xmin}
    - 이전 ctid: {old_ctid} → 새 ctid: {new_ctid}

    핵심 포인트:
    1. xmin이 변경됨 = 새로운 튜플이 생성되었다는 증거
    2. ctid가 변경됨 = 물리적 위치가 달라졌다는 증거
    3. UPDATE = 기존 튜플 삭제 표시 + 새 튜플 생성
        """)

    finally:
        cur.execute("DELETE FROM accounts WHERE id = %s", (test_id,))
        cur.close()
        conn.close()


def scenario_2_pageinspect_raw_tuples():
    """
    시나리오 2: pageinspect로 raw 튜플 확인
    --------------------------------------
    삭제 표시된 튜플과 새 튜플을 동시에 볼 수 있습니다.
    """
    print_section("시나리오 2: pageinspect로 raw 튜플 확인")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    # 전용 테스트 테이블 생성
    cur.execute("""
        DROP TABLE IF EXISTS update_test;
        CREATE TABLE update_test (
            id SERIAL PRIMARY KEY,
            value INTEGER
        );
        INSERT INTO update_test (value) VALUES (100);
    """)

    try:
        print("\n[초기 상태 - heap_page_items로 확인]")
        cur.execute("""
            SELECT
                lp as line_pointer,
                lp_off as offset,
                lp_len as length,
                t_xmin,
                t_xmax,
                t_ctid,
                CASE WHEN t_data IS NOT NULL THEN 'has data' ELSE 'no data' END as data_status
            FROM heap_page_items(get_raw_page('update_test', 0))
        """)
        print_result(cur, "페이지 0의 모든 튜플 (raw)")

        # UPDATE 실행
        print("\n[UPDATE 실행: value 100 → 200]")
        cur.execute("UPDATE update_test SET value = 200 WHERE id = 1")

        print("\n[UPDATE 후 - heap_page_items로 확인]")
        cur.execute("""
            SELECT
                lp as line_pointer,
                lp_off as offset,
                lp_len as length,
                t_xmin,
                t_xmax,
                t_ctid,
                CASE WHEN t_data IS NOT NULL THEN 'has data' ELSE 'no data' END as data_status
            FROM heap_page_items(get_raw_page('update_test', 0))
        """)
        rows = print_result(cur, "페이지 0의 모든 튜플 (raw)")

        if len(rows) >= 2:
            print("""
    분석:
    - line_pointer 1: 기존 튜플 (t_xmax가 설정됨 = 삭제 표시)
    - line_pointer 2: 새 튜플 (t_xmax = 0 = 활성 상태)

    핵심 포인트:
    기존 튜플은 물리적으로 여전히 존재하지만 t_xmax가 설정되어
    새로운 트랜잭션에서는 보이지 않습니다 (MVCC 원리).
    이런 튜플들이 'dead tuple'이 되어 VACUUM의 정리 대상이 됩니다.
            """)

        # 일반 SELECT는 새 튜플만 봄
        print("\n[일반 SELECT 결과]")
        cur.execute("SELECT xmin, xmax, ctid, * FROM update_test")
        print_result(cur, "일반 쿼리로 보이는 데이터 (새 튜플만)")

    finally:
        cur.execute("DROP TABLE IF EXISTS update_test")
        cur.close()
        conn.close()


def scenario_3_delete_marks_xmax():
    """
    시나리오 3: DELETE는 xmax를 설정한다
    -----------------------------------
    DELETE는 튜플을 물리적으로 삭제하지 않고 xmax만 설정합니다.
    """
    print_section("시나리오 3: DELETE는 xmax를 설정한다")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    # 테스트 테이블 생성
    cur.execute("""
        DROP TABLE IF EXISTS delete_test;
        CREATE TABLE delete_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        INSERT INTO delete_test (name) VALUES ('To Be Deleted');
    """)

    try:
        print("\n[DELETE 전 - heap_page_items]")
        cur.execute("""
            SELECT lp, t_xmin, t_xmax, t_ctid
            FROM heap_page_items(get_raw_page('delete_test', 0))
            WHERE t_data IS NOT NULL
        """)
        print_result(cur, "DELETE 전 튜플")

        # DELETE 실행
        print("\n[DELETE 실행]")
        cur.execute("SELECT txid_current()")
        delete_txid = cur.fetchone()[0]
        print(f"DELETE 트랜잭션 ID: {delete_txid}")

        cur.execute("DELETE FROM delete_test WHERE id = 1")

        print("\n[DELETE 후 - heap_page_items]")
        cur.execute("""
            SELECT lp, t_xmin, t_xmax, t_ctid
            FROM heap_page_items(get_raw_page('delete_test', 0))
            WHERE t_data IS NOT NULL
        """)
        rows = print_result(cur, "DELETE 후 튜플")

        if rows:
            print(f"""
    분석:
    - t_xmax = {rows[0][2]} (DELETE 트랜잭션 ID: {delete_txid})
    - 튜플이 물리적으로 여전히 존재!
    - t_xmax가 설정되어 '삭제됨'으로 표시만 됨

    핵심 포인트:
    PostgreSQL의 DELETE는 '논리적 삭제'입니다.
    실제 물리적 공간 회수는 VACUUM이 담당합니다.
            """)

        # 일반 SELECT 확인
        print("\n[일반 SELECT 결과]")
        cur.execute("SELECT * FROM delete_test")
        print_result(cur, "일반 쿼리 결과 (비어있음)")

    finally:
        cur.execute("DROP TABLE IF EXISTS delete_test")
        cur.close()
        conn.close()


def scenario_4_hot_update():
    """
    시나리오 4: HOT (Heap-Only Tuple) UPDATE
    ----------------------------------------
    인덱스 컬럼이 변경되지 않으면 HOT UPDATE가 발생할 수 있습니다.
    """
    print_section("시나리오 4: HOT (Heap-Only Tuple) UPDATE")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS hot_test;
        CREATE TABLE hot_test (
            id SERIAL PRIMARY KEY,
            indexed_col INTEGER,
            non_indexed_col INTEGER
        );
        CREATE INDEX idx_hot_test ON hot_test(indexed_col);
        INSERT INTO hot_test (indexed_col, non_indexed_col) VALUES (1, 100);
    """)

    try:
        print("\n[초기 상태]")
        cur.execute("""
            SELECT ctid, xmin, xmax, * FROM hot_test
        """)
        print_result(cur, "초기 튜플")

        # 인덱스되지 않은 컬럼 UPDATE (HOT 가능)
        print("\n[non_indexed_col UPDATE (인덱스 안 건드림)]")
        cur.execute("UPDATE hot_test SET non_indexed_col = 200 WHERE id = 1")

        cur.execute("""
            SELECT ctid, xmin, xmax, * FROM hot_test
        """)
        print_result(cur, "UPDATE 후 튜플")

        # HOT chain 확인
        print("\n[heap_page_items로 HOT chain 확인]")
        cur.execute("""
            SELECT lp, t_xmin, t_xmax, t_ctid,
                   CASE WHEN (t_infomask2 & 16384) != 0
                        THEN 'HOT updated' ELSE 'normal' END as hot_status
            FROM heap_page_items(get_raw_page('hot_test', 0))
            WHERE t_data IS NOT NULL
        """)
        print_result(cur, "HOT chain 상태")

        print("""
    HOT UPDATE란?
    - 인덱스 컬럼이 변경되지 않을 때 인덱스를 건드리지 않는 최적화
    - 새 튜플이 같은 페이지에 생성되고, 기존 튜플이 새 튜플을 가리킴
    - 인덱스 bloat을 줄여 성능 향상에 기여
        """)

    finally:
        cur.execute("DROP TABLE IF EXISTS hot_test")
        cur.close()
        conn.close()


def main():
    """메인 실행 함수"""
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║        Lab 02: UPDATE/DELETE 시 튜플 변화 실습              ║
    ║                                                           ║
    ║  UPDATE = DELETE + INSERT 원리를 직접 확인합니다.           ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    try:
        scenario_1_update_creates_new_tuple()
        scenario_2_pageinspect_raw_tuples()
        scenario_3_delete_marks_xmax()
        scenario_4_hot_update()

        print_section("Lab 02 완료!")
        print("""
    학습 정리:
    1. UPDATE는 기존 튜플의 xmax를 설정하고 새 튜플을 생성
    2. DELETE는 xmax만 설정 (물리적 삭제 X)
    3. pageinspect로 "삭제된" 튜플도 볼 수 있음
    4. Dead tuple들은 VACUUM이 정리
    5. HOT UPDATE는 인덱스 bloat을 줄이는 최적화

    다음 실습: lab03_isolation.py
        """)

    except psycopg2.OperationalError as e:
        print(f"\n오류: 데이터베이스에 연결할 수 없습니다.")
        print(f"Docker가 실행 중인지 확인하세요: docker-compose up -d")
        print(f"상세 오류: {e}")


if __name__ == "__main__":
    main()
