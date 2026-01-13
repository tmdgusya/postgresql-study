"""
Lab 01: xmin/xmax 기초 실습
============================

학습 목표:
- PostgreSQL 튜플의 시스템 컬럼(xmin, xmax, ctid) 이해
- 트랜잭션 ID와 튜플 가시성의 관계 파악
- 커밋 전후의 가시성 차이 확인

실행 방법:
    python lab01_xmin_xmax.py
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


def get_connection(autocommit=False):
    """새 데이터베이스 연결 생성"""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = autocommit
    return conn


def print_section(title):
    """섹션 구분선 출력"""
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print('=' * 60)


def print_result(cursor, description=""):
    """쿼리 결과를 테이블 형식으로 출력"""
    if description:
        print(f"\n>> {description}")
    rows = cursor.fetchall()
    if rows:
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt='psql'))
    else:
        print("(결과 없음)")
    return rows


def scenario_1_basic_system_columns():
    """
    시나리오 1: 기본 시스템 컬럼 확인
    ---------------------------------
    xmin, xmax, ctid가 무엇인지 직접 확인합니다.
    """
    print_section("시나리오 1: 기본 시스템 컬럼 확인")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    # 현재 데이터의 시스템 컬럼 확인
    print("\n[accounts 테이블의 시스템 컬럼]")
    cur.execute("""
        SELECT
            xmin,           -- 이 튜플을 INSERT한 트랜잭션 ID
            xmax,           -- 이 튜플을 DELETE/UPDATE한 트랜잭션 ID (0이면 아직 안됨)
            ctid,           -- 튜플의 물리적 위치 (page, offset)
            id, name, balance
        FROM accounts
        ORDER BY id
    """)
    print_result(cur, "xmin, xmax, ctid 컬럼 확인")

    print("""
    해석:
    - xmin: 각 row를 INSERT한 트랜잭션의 ID
    - xmax: 0이면 아직 삭제/수정되지 않은 '살아있는' 튜플
    - ctid: (페이지 번호, 오프셋) 형태의 물리적 위치
    """)

    # 현재 트랜잭션 ID 확인
    cur.execute("SELECT txid_current()")
    txid = cur.fetchone()[0]
    print(f"\n현재 트랜잭션 ID: {txid}")
    print("(새로운 INSERT/UPDATE/DELETE 시 이 ID가 xmin 또는 xmax에 기록됩니다)")

    cur.close()
    conn.close()


def scenario_2_visibility_before_commit():
    """
    시나리오 2: 커밋 전후 가시성 확인
    ---------------------------------
    커밋되지 않은 데이터는 다른 트랜잭션에서 보이지 않습니다.
    """
    print_section("시나리오 2: 커밋 전후 가시성 확인")

    # 두 개의 독립적인 연결 (서로 다른 트랜잭션)
    conn_a = get_connection()  # 세션 A
    conn_b = get_connection(autocommit=True)  # 세션 B (자동 커밋)

    cur_a = conn_a.cursor()
    cur_b = conn_b.cursor()

    try:
        # 세션 A: 트랜잭션 시작하고 INSERT
        print("\n[세션 A] BEGIN 후 INSERT (커밋 안함)")
        cur_a.execute("BEGIN")
        cur_a.execute("SELECT txid_current()")
        txid_a = cur_a.fetchone()[0]
        print(f"세션 A 트랜잭션 ID: {txid_a}")

        cur_a.execute("""
            INSERT INTO accounts (name, balance)
            VALUES ('New User (uncommitted)', 9999)
            RETURNING xmin, xmax, ctid, id, name, balance
        """)
        print_result(cur_a, "INSERT 결과 (세션 A에서 본 것)")

        # 세션 B: 세션 A가 커밋하기 전에 SELECT
        print("\n[세션 B] 세션 A가 커밋하기 전에 SELECT")
        cur_b.execute("""
            SELECT xmin, xmax, ctid, id, name, balance
            FROM accounts
            ORDER BY id
        """)
        print_result(cur_b, "세션 B에서 본 데이터")
        print("\n--> 'New User (uncommitted)'가 보이지 않습니다!")
        print("    이유: 세션 A의 트랜잭션이 아직 커밋되지 않았기 때문")

        # 세션 A: 커밋
        print("\n[세션 A] COMMIT 실행")
        conn_a.commit()

        # 세션 B: 커밋 후 다시 SELECT
        print("\n[세션 B] 세션 A 커밋 후 다시 SELECT")
        cur_b.execute("""
            SELECT xmin, xmax, ctid, id, name, balance
            FROM accounts
            ORDER BY id
        """)
        print_result(cur_b, "세션 B에서 본 데이터")
        print("\n--> 이제 'New User (uncommitted)'가 보입니다!")
        print(f"    xmin = {txid_a} (세션 A의 트랜잭션 ID와 동일)")

    finally:
        # 정리: 테스트 데이터 삭제
        cur_b.execute("DELETE FROM accounts WHERE name = 'New User (uncommitted)'")
        cur_a.close()
        cur_b.close()
        conn_a.close()
        conn_b.close()


def scenario_3_rollback_visibility():
    """
    시나리오 3: ROLLBACK 시 가시성
    -----------------------------
    롤백된 트랜잭션의 데이터는 영원히 보이지 않습니다.
    """
    print_section("시나리오 3: ROLLBACK 시 가시성")

    conn_a = get_connection()
    conn_b = get_connection(autocommit=True)

    cur_a = conn_a.cursor()
    cur_b = conn_b.cursor()

    try:
        # 세션 A: INSERT 후 ROLLBACK
        print("\n[세션 A] BEGIN 후 INSERT")
        cur_a.execute("BEGIN")
        cur_a.execute("SELECT txid_current()")
        txid_a = cur_a.fetchone()[0]
        print(f"세션 A 트랜잭션 ID: {txid_a}")

        cur_a.execute("""
            INSERT INTO accounts (name, balance)
            VALUES ('Rollback User', 7777)
            RETURNING xmin, xmax, ctid, id, name, balance
        """)
        print_result(cur_a, "INSERT 결과")

        # 롤백
        print("\n[세션 A] ROLLBACK 실행")
        conn_a.rollback()

        # 세션 B: 롤백 후 확인
        print("\n[세션 B] 롤백 후 SELECT")
        cur_b.execute("""
            SELECT xmin, xmax, ctid, id, name, balance
            FROM accounts
            WHERE name = 'Rollback User'
        """)
        print_result(cur_b, "세션 B에서 본 데이터")
        print("\n--> 'Rollback User'는 영원히 보이지 않습니다!")
        print("    롤백된 트랜잭션의 xmin을 가진 튜플은 모든 트랜잭션에서 invisible")

    finally:
        cur_a.close()
        cur_b.close()
        conn_a.close()
        conn_b.close()


def scenario_4_transaction_id_sequence():
    """
    시나리오 4: 트랜잭션 ID 시퀀스 확인
    -----------------------------------
    트랜잭션 ID가 어떻게 증가하는지 확인합니다.
    """
    print_section("시나리오 4: 트랜잭션 ID 시퀀스")

    conn = get_connection()
    cur = conn.cursor()

    txids = []

    print("\n여러 번 트랜잭션을 실행하며 ID 확인:")
    for i in range(5):
        cur.execute("BEGIN")
        cur.execute("SELECT txid_current()")
        txid = cur.fetchone()[0]
        txids.append(txid)
        print(f"  트랜잭션 {i+1}: txid = {txid}")
        conn.commit()

    print(f"\n트랜잭션 ID 증가량: {[txids[i+1] - txids[i] for i in range(len(txids)-1)]}")
    print("(ID는 순차적으로 증가하며, 각 트랜잭션마다 새 ID가 할당됩니다)")

    cur.close()
    conn.close()


def main():
    """메인 실행 함수"""
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║           Lab 01: xmin/xmax 기초 실습                      ║
    ║                                                           ║
    ║  PostgreSQL 튜플의 시스템 컬럼과 트랜잭션 가시성을 학습합니다.  ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    try:
        scenario_1_basic_system_columns()
        scenario_2_visibility_before_commit()
        scenario_3_rollback_visibility()
        scenario_4_transaction_id_sequence()

        print_section("Lab 01 완료!")
        print("""
    학습 정리:
    1. xmin: 튜플을 생성한 트랜잭션 ID
    2. xmax: 튜플을 삭제/수정한 트랜잭션 ID (0이면 활성 상태)
    3. ctid: 튜플의 물리적 위치 (page, offset)
    4. 커밋되지 않은 트랜잭션의 변경은 다른 트랜잭션에서 보이지 않음
    5. 롤백된 트랜잭션의 변경은 영원히 보이지 않음

    다음 실습: lab02_update_delete.py
        """)

    except psycopg2.OperationalError as e:
        print(f"\n오류: 데이터베이스에 연결할 수 없습니다.")
        print(f"Docker가 실행 중인지 확인하세요: docker-compose up -d")
        print(f"상세 오류: {e}")


if __name__ == "__main__":
    main()
