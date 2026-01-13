"""
Lab 02b: Snapshot 이해 실습
===========================

학습 목표:
- PostgreSQL Snapshot의 구조 이해 (xmin, xmax, xip[])
- 스냅샷으로 튜플 가시성이 어떻게 결정되는지 확인
- 격리 수준별 스냅샷 생성 시점 차이 체험

실행 방법:
    python lab02b_snapshot.py

주의: 이 실습은 PostgreSQL 13+ 에서 pg_current_snapshot() 함수를 사용합니다.
"""

import psycopg2
from tabulate import tabulate
import threading
import time

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


def scenario_1_snapshot_structure():
    """
    시나리오 1: Snapshot 구조 이해
    -----------------------------
    pg_current_snapshot()으로 스냅샷의 구성 요소를 확인합니다.
    """
    print_section("시나리오 1: Snapshot 구조 이해")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    try:
        print("""
    Snapshot의 형식: xmin:xmax:xip_list

    • xmin  : 아직 진행 중인 가장 오래된 트랜잭션 ID
    • xmax  : 다음에 할당될 트랜잭션 ID
    • xip[] : 스냅샷 생성 시점에 진행 중이던 트랜잭션 목록
        """)

        # 현재 스냅샷 확인
        cur.execute("SELECT pg_current_snapshot()")
        snapshot = cur.fetchone()[0]
        print(f"\n현재 스냅샷: {snapshot}")

        # 스냅샷 구성 요소 분해
        cur.execute("""
            SELECT
                pg_snapshot_xmin(pg_current_snapshot()) as xmin,
                pg_snapshot_xmax(pg_current_snapshot()) as xmax
        """)
        print_result(cur, "스냅샷 구성 요소")

        # xip 목록 확인 (진행 중인 트랜잭션이 있을 경우)
        cur.execute("""
            SELECT pg_snapshot_xip(pg_current_snapshot()) as in_progress_xids
        """)
        result = cur.fetchall()
        if result and result[0][0] is not None:
            print(f"\n진행 중인 트랜잭션 목록: {result}")
        else:
            print("\n진행 중인 다른 트랜잭션 없음 (xip[] 비어있음)")

        print("""
    해석:
    - xmin보다 작은 트랜잭션: 이미 완료됨 (커밋 또는 abort)
    - xmax보다 큰 트랜잭션: 아직 시작 안됨 (미래)
    - xip[]에 있는 트랜잭션: 스냅샷 생성 시점에 진행 중이었음
        """)

    finally:
        cur.close()
        conn.close()


def scenario_2_snapshot_with_active_transactions():
    """
    시나리오 2: 다른 트랜잭션이 있을 때의 Snapshot
    ---------------------------------------------
    진행 중인 트랜잭션이 스냅샷에 어떻게 반영되는지 확인합니다.
    """
    print_section("시나리오 2: 진행 중인 트랜잭션과 Snapshot")

    conn_main = get_connection(autocommit=True)  # 스냅샷 관찰용
    conn_t1 = get_connection()  # 진행 중인 트랜잭션 1
    conn_t2 = get_connection()  # 진행 중인 트랜잭션 2

    cur_main = conn_main.cursor()
    cur_t1 = conn_t1.cursor()
    cur_t2 = conn_t2.cursor()

    try:
        # 초기 스냅샷
        cur_main.execute("SELECT pg_current_snapshot()")
        snapshot_before = cur_main.fetchone()[0]
        print(f"\n[초기] 스냅샷: {snapshot_before}")

        # T1 시작 (커밋 안함)
        print("\n[T1] BEGIN - 트랜잭션 시작")
        cur_t1.execute("BEGIN")
        cur_t1.execute("SELECT txid_current()")
        t1_xid = cur_t1.fetchone()[0]
        print(f"[T1] 트랜잭션 ID: {t1_xid}")

        # 스냅샷 변화 확인
        cur_main.execute("SELECT pg_current_snapshot()")
        snapshot_after_t1 = cur_main.fetchone()[0]
        print(f"\n[T1 시작 후] 스냅샷: {snapshot_after_t1}")

        # T2 시작 (커밋 안함)
        print("\n[T2] BEGIN - 트랜잭션 시작")
        cur_t2.execute("BEGIN")
        cur_t2.execute("SELECT txid_current()")
        t2_xid = cur_t2.fetchone()[0]
        print(f"[T2] 트랜잭션 ID: {t2_xid}")

        # 스냅샷 변화 확인
        cur_main.execute("SELECT pg_current_snapshot()")
        snapshot_after_t2 = cur_main.fetchone()[0]
        print(f"\n[T2 시작 후] 스냅샷: {snapshot_after_t2}")

        # xip 목록 확인
        cur_main.execute("SELECT pg_snapshot_xip(pg_current_snapshot())")
        xip_list = list(cur_main.fetchall())
        print(f"\n진행 중인 트랜잭션 (xip[]): {xip_list}")

        print(f"""
    분석:
    - T1 (xid={t1_xid})과 T2 (xid={t2_xid})가 진행 중
    - 이 트랜잭션들은 xip[] 목록에 포함됨
    - 다른 트랜잭션에서 스냅샷을 생성하면 T1, T2의 변경은 보이지 않음
        """)

        # T1 커밋
        print("[T1] COMMIT")
        conn_t1.commit()

        # 스냅샷 변화 확인
        cur_main.execute("SELECT pg_current_snapshot()")
        snapshot_after_commit = cur_main.fetchone()[0]
        print(f"\n[T1 커밋 후] 스냅샷: {snapshot_after_commit}")

        # 정리
        conn_t2.rollback()

    finally:
        cur_main.close()
        cur_t1.close()
        cur_t2.close()
        conn_main.close()
        conn_t1.close()
        conn_t2.close()


def scenario_3_visibility_rules():
    """
    시나리오 3: 스냅샷 기반 가시성 규칙
    ----------------------------------
    스냅샷의 xmin, xmax, xip[]로 튜플 가시성이 어떻게 결정되는지 확인합니다.
    """
    print_section("시나리오 3: 스냅샷 기반 가시성 규칙")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    # 테스트 테이블 생성
    cur.execute("""
        DROP TABLE IF EXISTS snapshot_test;
        CREATE TABLE snapshot_test (
            id SERIAL PRIMARY KEY,
            label VARCHAR(50)
        );
    """)

    try:
        print("""
    가시성 판단 규칙:

    튜플이 VISIBLE 하려면:
    ┌─────────────────────────────────────────────────────────┐
    │ 1. xmin이 "커밋됨"으로 판단되어야 함                      │
    │    - xmin < snapshot.xmin → 커밋됨                       │
    │    - xmin >= snapshot.xmax → 미래 (안 보임)              │
    │    - xmin IN xip[] → 진행 중 (안 보임)                   │
    │    - 그 외 → 커밋됨                                      │
    │                                                          │
    │ 2. xmax가 없거나 "커밋 안됨"이어야 함                     │
    │    - xmax = 0 → 삭제 안됨 (보임)                         │
    │    - xmax >= snapshot.xmax → 미래 삭제 (보임)            │
    │    - xmax IN xip[] → 삭제 진행 중 (보임)                 │
    │    - 그 외 (xmax 커밋됨) → 삭제됨 (안 보임)              │
    └─────────────────────────────────────────────────────────┘
        """)

        # 데이터 INSERT
        cur.execute("INSERT INTO snapshot_test (label) VALUES ('old_data')")
        cur.execute("SELECT xmin, xmax, ctid, * FROM snapshot_test")
        print_result(cur, "INSERT 후 튜플 상태")

        # 현재 스냅샷 확인
        cur.execute("""
            SELECT
                pg_snapshot_xmin(pg_current_snapshot()) as snap_xmin,
                pg_snapshot_xmax(pg_current_snapshot()) as snap_xmax
        """)
        snap = cur.fetchone()
        print(f"\n현재 스냅샷: xmin={snap[0]}, xmax={snap[1]}")

        # 튜플의 xmin과 스냅샷 비교
        cur.execute("SELECT xmin FROM snapshot_test WHERE id = 1")
        tuple_xmin = cur.fetchone()[0]

        print(f"""
    분석:
    - 튜플의 xmin: {tuple_xmin}
    - 스냅샷 xmin: {snap[0]}
    - 스냅샷 xmax: {snap[1]}

    판단:
    - {tuple_xmin} < {snap[0]} (스냅샷 xmin) ?
    - 예 → 이 트랜잭션은 확실히 커밋됨 → 튜플 VISIBLE
        """)

    finally:
        cur.execute("DROP TABLE IF EXISTS snapshot_test")
        cur.close()
        conn.close()


def scenario_4_read_committed_snapshot():
    """
    시나리오 4: READ COMMITTED의 스냅샷 동작
    ---------------------------------------
    각 쿼리마다 새로운 스냅샷이 생성되는 것을 확인합니다.
    """
    print_section("시나리오 4: READ COMMITTED의 스냅샷 동작")

    conn_reader = get_connection()  # READ COMMITTED (기본값)
    conn_writer = get_connection(autocommit=True)

    cur_reader = conn_reader.cursor()
    cur_writer = conn_writer.cursor()

    # 테스트 데이터 준비
    cur_writer.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")

    try:
        print("""
    READ COMMITTED: 각 SELECT마다 새로운 스냅샷 생성
        """)

        # 첫 번째 SELECT
        cur_reader.execute("BEGIN")
        cur_reader.execute("SELECT pg_current_snapshot()")
        snap1 = cur_reader.fetchone()[0]
        cur_reader.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance1 = cur_reader.fetchone()[0]
        print(f"[SELECT 1] 스냅샷: {snap1}, 잔액: {balance1}")

        # 다른 트랜잭션에서 변경
        print("\n[다른 세션] Alice 잔액 변경: 1000 → 500")
        cur_writer.execute("UPDATE accounts SET balance = 500 WHERE name = 'Alice'")

        # 두 번째 SELECT
        cur_reader.execute("SELECT pg_current_snapshot()")
        snap2 = cur_reader.fetchone()[0]
        cur_reader.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance2 = cur_reader.fetchone()[0]
        print(f"[SELECT 2] 스냅샷: {snap2}, 잔액: {balance2}")

        conn_reader.rollback()

        print(f"""
    분석:
    - 첫 번째 SELECT의 스냅샷: {snap1}
    - 두 번째 SELECT의 스냅샷: {snap2}
    - 스냅샷이 다름! → 각 쿼리마다 새로 생성됨
    - 결과: {balance1} → {balance2} (값이 변경됨)

    READ COMMITTED에서는 각 SELECT가 실행될 때마다
    그 시점의 커밋된 데이터를 기준으로 새 스냅샷을 만듭니다.
        """)

    finally:
        cur_writer.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
        cur_reader.close()
        cur_writer.close()
        conn_reader.close()
        conn_writer.close()


def scenario_5_repeatable_read_snapshot():
    """
    시나리오 5: REPEATABLE READ의 스냅샷 동작
    ----------------------------------------
    트랜잭션 시작 시점의 스냅샷이 계속 유지되는 것을 확인합니다.
    """
    print_section("시나리오 5: REPEATABLE READ의 스냅샷 동작")

    conn_reader = get_connection()
    conn_writer = get_connection(autocommit=True)

    cur_reader = conn_reader.cursor()
    cur_writer = conn_writer.cursor()

    cur_writer.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")

    try:
        print("""
    REPEATABLE READ: 첫 SELECT에서 스냅샷 생성, 이후 재사용
        """)

        # REPEATABLE READ 시작
        cur_reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")

        # 첫 번째 SELECT (여기서 스냅샷 고정!)
        cur_reader.execute("SELECT pg_current_snapshot()")
        snap1 = cur_reader.fetchone()[0]
        cur_reader.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance1 = cur_reader.fetchone()[0]
        print(f"[SELECT 1] 스냅샷: {snap1}, 잔액: {balance1}")

        # 다른 트랜잭션에서 변경
        print("\n[다른 세션] Alice 잔액 변경: 1000 → 500")
        cur_writer.execute("UPDATE accounts SET balance = 500 WHERE name = 'Alice'")

        # 두 번째 SELECT
        cur_reader.execute("SELECT pg_current_snapshot()")
        snap2 = cur_reader.fetchone()[0]
        cur_reader.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance2 = cur_reader.fetchone()[0]
        print(f"[SELECT 2] 스냅샷: {snap2}, 잔액: {balance2}")

        conn_reader.rollback()

        print(f"""
    분석:
    - 첫 번째 SELECT의 스냅샷: {snap1}
    - 두 번째 SELECT의 스냅샷: {snap2}
    - 스냅샷이 동일! → 트랜잭션 시작 시점에 고정됨
    - 결과: {balance1} → {balance2} (값이 동일!)

    REPEATABLE READ에서는 트랜잭션의 첫 쿼리 시점에
    스냅샷이 생성되고, 트랜잭션이 끝날 때까지 유지됩니다.
    다른 트랜잭션이 커밋해도 이 스냅샷에는 반영되지 않습니다.
        """)

    finally:
        cur_writer.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
        cur_reader.close()
        cur_writer.close()
        conn_reader.close()
        conn_writer.close()


def scenario_6_snapshot_and_xip():
    """
    시나리오 6: xip[]의 역할 이해
    ---------------------------
    진행 중인 트랜잭션이 xip[]에 어떻게 영향을 주는지 확인합니다.
    """
    print_section("시나리오 6: xip[] (진행 중인 트랜잭션 목록)")

    conn_main = get_connection(autocommit=True)
    conn_t1 = get_connection()
    conn_t2 = get_connection()
    conn_t3 = get_connection()

    cur_main = conn_main.cursor()
    cur_t1 = conn_t1.cursor()
    cur_t2 = conn_t2.cursor()
    cur_t3 = conn_t3.cursor()

    try:
        print("""
    xip[] = 스냅샷 생성 시점에 "진행 중"이던 트랜잭션 목록

    이 목록에 있는 트랜잭션의 변경은 보이지 않습니다.
    (아직 커밋되지 않았으므로)
        """)

        # T1 시작
        cur_t1.execute("BEGIN")
        cur_t1.execute("SELECT txid_current()")
        t1_xid = cur_t1.fetchone()[0]
        print(f"\n[T1] 시작, xid = {t1_xid}")

        # T2 시작
        cur_t2.execute("BEGIN")
        cur_t2.execute("SELECT txid_current()")
        t2_xid = cur_t2.fetchone()[0]
        print(f"[T2] 시작, xid = {t2_xid}")

        # T3 시작
        cur_t3.execute("BEGIN")
        cur_t3.execute("SELECT txid_current()")
        t3_xid = cur_t3.fetchone()[0]
        print(f"[T3] 시작, xid = {t3_xid}")

        # T2 커밋
        print(f"\n[T2] COMMIT")
        conn_t2.commit()

        # 이 시점의 스냅샷 확인
        cur_main.execute("SELECT pg_current_snapshot()")
        snapshot = cur_main.fetchone()[0]
        print(f"\n현재 스냅샷: {snapshot}")

        cur_main.execute("""
            SELECT
                pg_snapshot_xmin(pg_current_snapshot()) as xmin,
                pg_snapshot_xmax(pg_current_snapshot()) as xmax
        """)
        snap_info = cur_main.fetchone()

        # xip 조회
        cur_main.execute("SELECT pg_snapshot_xip(pg_current_snapshot())")
        xip_rows = cur_main.fetchall()
        xip_list = [row[0] for row in xip_rows] if xip_rows else []

        print(f"""
    스냅샷 분석:
    - xmin = {snap_info[0]} (가장 오래된 활성 트랜잭션)
    - xmax = {snap_info[1]} (다음 할당될 트랜잭션 ID)
    - xip[] = {xip_list} (진행 중인 트랜잭션)

    트랜잭션 상태:
    - T1 (xid={t1_xid}): 진행 중 → xip[]에 포함
    - T2 (xid={t2_xid}): 커밋됨 → xip[]에 없음
    - T3 (xid={t3_xid}): 진행 중 → xip[]에 포함

    가시성:
    - T1의 변경: 보이지 않음 (xip[]에 있으므로)
    - T2의 변경: 보임 (커밋됨, xip[]에 없음)
    - T3의 변경: 보이지 않음 (xip[]에 있으므로)
        """)

        # 정리
        conn_t1.rollback()
        conn_t3.rollback()

    finally:
        cur_main.close()
        cur_t1.close()
        cur_t2.close()
        cur_t3.close()
        conn_main.close()
        conn_t1.close()
        conn_t2.close()
        conn_t3.close()


def main():
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║              Lab 02b: Snapshot 이해 실습                   ║
    ║                                                           ║
    ║  스냅샷이 무엇이고 격리 수준과 어떻게 연결되는지 학습합니다.  ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    try:
        scenario_1_snapshot_structure()
        scenario_2_snapshot_with_active_transactions()
        scenario_3_visibility_rules()
        scenario_4_read_committed_snapshot()
        scenario_5_repeatable_read_snapshot()
        scenario_6_snapshot_and_xip()

        print_section("Lab 02b 완료!")
        print("""
    학습 정리:

    1. Snapshot 구조:
       - xmin: 가장 오래된 활성 트랜잭션
       - xmax: 다음 할당될 트랜잭션 ID
       - xip[]: 진행 중인 트랜잭션 목록

    2. 가시성 규칙:
       - xmin < snap.xmin → 확실히 커밋됨
       - xmin >= snap.xmax → 미래 (안 보임)
       - xmin IN xip[] → 진행 중 (안 보임)

    3. 격리 수준별 차이:
       - READ COMMITTED: 각 쿼리마다 새 스냅샷
       - REPEATABLE READ: 트랜잭션 전체에서 하나의 스냅샷

    4. 핵심 포인트:
       격리 수준의 차이 = 스냅샷 생성 시점의 차이!

    다음 실습: lab03_isolation.py
        """)

    except psycopg2.OperationalError as e:
        print(f"\n오류: 데이터베이스에 연결할 수 없습니다.")
        print(f"Docker가 실행 중인지 확인하세요: docker-compose up -d")
        print(f"상세 오류: {e}")


if __name__ == "__main__":
    main()
