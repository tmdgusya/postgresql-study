"""
Lab 00: 가시성 체험 - "같은 순간, 다른 현실"
=============================================

학습 목표:
- MVCC 가시성의 핵심을 직접 체험
- "커밋됐는데도 안 보인다"는 상황 이해
- 세션마다 다른 현실을 보는 것을 확인

이 lab은 다른 lab들보다 먼저 실행하는 것을 권장합니다.
가시성의 "아하!" 순간을 경험할 수 있습니다.

실행 방법:
    python lab00_visibility_experience.py
"""

import psycopg2
from tabulate import tabulate
import time
import threading

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'mvcc_lab',
    'user': 'study',
    'password': 'study123'
}


def get_connection():
    """새 데이터베이스 연결 생성 (항상 autocommit=False)"""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def print_snapshot(cursor, session_name):
    """현재 스냅샷 상태 출력"""
    cursor.execute("""
        SELECT
            pg_current_snapshot() as snapshot,
            pg_snapshot_xmin(pg_current_snapshot()) as xmin,
            pg_snapshot_xmax(pg_current_snapshot()) as xmax
    """)
    row = cursor.fetchone()
    print(f"\n  [{session_name}] 스냅샷: {row[0]}")
    print(f"           xmin={row[1]} (이보다 작은 xid는 완료됨)")
    print(f"           xmax={row[2]} (이보다 큰 xid는 미래)")

    # xip[] 출력 (진행 중인 트랜잭션)
    cursor.execute("SELECT pg_snapshot_xip(pg_current_snapshot())")
    xip = [r[0] for r in cursor.fetchall()]
    if xip:
        print(f"           xip={xip} (진행 중인 트랜잭션)")
    else:
        print(f"           xip=[] (진행 중인 트랜잭션 없음)")


def print_section(title):
    print(f"\n{'=' * 70}")
    print(f" {title}")
    print('=' * 70)


def print_box(lines, emoji=""):
    """박스 형태로 메시지 출력"""
    max_len = max(len(line) for line in lines)
    print(f"\n  {'─' * (max_len + 4)}")
    for line in lines:
        print(f"  │ {emoji} {line.ljust(max_len)} │")
    print(f"  {'─' * (max_len + 4)}")


def wait_for_user(message="계속하려면 Enter를 누르세요..."):
    """사용자 입력 대기"""
    print(f"\n  ⏸️  {message}")
    input()


def scenario_1_parallel_universes():
    """
    시나리오 1: 평행 우주
    --------------------
    같은 순간, 세션 A와 세션 B가 다른 결과를 본다!
    """
    print_section("시나리오 1: 평행 우주 (Parallel Universes)")

    print("""
    이 시나리오에서 당신은 "평행 우주"를 체험합니다.
    같은 테이블, 같은 순간에 세션마다 다른 결과를 보게 됩니다.

    준비:
    - 세션 A: REPEATABLE READ로 과거에 고정
    - 세션 B: 새 데이터를 추가하고 커밋
    - 세션 C: 현재 상태를 확인
    """)

    # 정리: Ghost 데이터가 있다면 삭제
    conn_cleanup = get_connection()
    cur_cleanup = conn_cleanup.cursor()
    cur_cleanup.execute("BEGIN")
    cur_cleanup.execute("DELETE FROM accounts WHERE name = 'Ghost'")
    conn_cleanup.commit()
    cur_cleanup.close()
    conn_cleanup.close()

    # 세 개의 세션 준비
    conn_a = get_connection()  # 세션 A: REPEATABLE READ
    conn_b = get_connection()  # 세션 B: 변경자
    conn_c = get_connection()  # 세션 C: 관찰자

    cur_a = conn_a.cursor()
    cur_b = conn_b.cursor()
    cur_c = conn_c.cursor()

    try:
        # Step 1: 세션 A가 스냅샷을 고정
        print("\n" + "─" * 70)
        print("  [Step 1] 세션 A: REPEATABLE READ로 스냅샷 고정")
        print("─" * 70)

        cur_a.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        cur_a.execute("SELECT COUNT(*) as count FROM accounts")
        count_a_before = cur_a.fetchone()[0]
        print(f"\n  세션 A가 본 데이터 개수: {count_a_before}건")

        # 스냅샷 상태 출력
        print_snapshot(cur_a, "세션 A")
        print("\n  └─ 이 스냅샷이 트랜잭션이 끝날 때까지 고정됩니다!")

        wait_for_user()

        # Step 2: 세션 B가 새 데이터 추가
        print("\n" + "─" * 70)
        print("  [Step 2] 세션 B: 새 데이터 'Ghost' 추가")
        print("─" * 70)

        print("\n  세션 B: BEGIN")
        cur_b.execute("BEGIN")

        cur_b.execute("""
            INSERT INTO accounts (name, balance)
            VALUES ('Ghost', 999)
            RETURNING xmin, id, name, balance
        """)
        result = cur_b.fetchone()
        print(f"  세션 B: INSERT 완료!")
        print(f"          xmin={result[0]}, id={result[1]}, name='{result[2]}', balance={result[3]}")

        print("\n  세션 B: COMMIT")
        conn_b.commit()
        print("  세션 B: COMMIT 완료!")

        # 세션 B의 스냅샷 확인 (새 트랜잭션)
        cur_b.execute("BEGIN")
        print_snapshot(cur_b, "세션 B")
        conn_b.commit()

        wait_for_user()

        # Step 3: 각 세션에서 COUNT 확인 - 핵심 순간!
        print("\n" + "─" * 70)
        print("  [Step 3] 같은 순간, 다른 현실! (핵심)")
        print("─" * 70)

        # 세션 A 스냅샷 확인 (여전히 고정)
        print_snapshot(cur_a, "세션 A")
        print("  └─ 세션 A의 스냅샷은 Step 1에서 고정된 그대로!")

        # 세션 C 스냅샷 확인 (새 트랜잭션)
        cur_c.execute("BEGIN")
        print_snapshot(cur_c, "세션 C")
        print("  └─ 세션 C는 새 스냅샷을 가져서 Ghost의 커밋이 반영됨!")

        # 세션 A 조회
        cur_a.execute("SELECT COUNT(*) as count FROM accounts")
        count_a_after = cur_a.fetchone()[0]

        # 세션 C 조회
        cur_c.execute("SELECT COUNT(*) as count FROM accounts")
        count_c = cur_c.fetchone()[0]
        conn_c.commit()

        print(f"""
  ┌────────────────────────────────────────────────────────────────┐
  │                   🌌 평행 우주 순간!                           │
  ├────────────────────────────────────────────────────────────────┤
  │                                                                │
  │   세션 A (REPEATABLE READ):  {count_a_after}건                         │
  │   세션 C (새 트랜잭션):       {count_c}건                         │
  │                                                                │
  │   👆 같은 테이블, 같은 순간인데 결과가 다릅니다!                │
  │                                                                │
  │   세션 A의 xmax보다 Ghost의 xmin이 크거나 같으므로 안 보임!     │
  │   세션 C의 xmax는 Ghost의 xmin보다 크므로 보임!                 │
  │                                                                │
  └────────────────────────────────────────────────────────────────┘
        """)

        # Ghost 확인
        print("\n  [세션 A] 'Ghost' 검색:")
        cur_a.execute("SELECT xmin, * FROM accounts WHERE name = 'Ghost'")
        result_a = cur_a.fetchall()
        if result_a:
            print(f"    결과: Ghost 발견! (xmin={result_a[0][0]})")
        else:
            print("    결과: (없음) - Ghost가 보이지 않습니다!")

        print("\n  [세션 C] 'Ghost' 검색:")
        cur_c.execute("BEGIN")
        cur_c.execute("SELECT xmin, * FROM accounts WHERE name = 'Ghost'")
        result_c = cur_c.fetchall()
        conn_c.commit()
        if result_c:
            print(f"    결과: Ghost 발견! (xmin={result_c[0][0]}, id={result_c[0][1]}, balance={result_c[0][3]})")
        else:
            print("    결과: (없음)")

        wait_for_user()

        # Step 4: 세션 A 커밋 후
        print("\n" + "─" * 70)
        print("  [Step 4] 세션 A: COMMIT 후 새 트랜잭션에서 확인")
        print("─" * 70)

        conn_a.commit()
        print("\n  세션 A: COMMIT 완료!")

        cur_a.execute("BEGIN")
        print_snapshot(cur_a, "세션 A (새 트랜잭션)")

        cur_a.execute("SELECT COUNT(*) as count FROM accounts")
        count_a_new = cur_a.fetchone()[0]
        conn_a.commit()
        print(f"\n  세션 A: 이제 {count_a_new}건이 보입니다!")
        print("  └─ 새 트랜잭션이므로 새로운 스냅샷을 봅니다.")

        print_box([
            "핵심 교훈:",
            "- REPEATABLE READ는 트랜잭션 시작 시점의 스냅샷을 고정합니다",
            "- 스냅샷의 xmax보다 큰 xmin을 가진 튜플은 보이지 않습니다",
            "- 같은 순간에 다른 세션이 다른 현실을 볼 수 있습니다",
        ])

    finally:
        # 정리
        cur_b.execute("BEGIN")
        cur_b.execute("DELETE FROM accounts WHERE name = 'Ghost'")
        conn_b.commit()
        cur_a.close()
        cur_b.close()
        cur_c.close()
        conn_a.close()
        conn_b.close()
        conn_c.close()


def scenario_2_ghost_delete():
    """
    시나리오 2: 유령 삭제
    --------------------
    다른 세션이 삭제했는데, 내 세션에서는 아직 보인다!
    """
    print_section("시나리오 2: 유령 삭제 (Ghost Delete)")

    print("""
    이 시나리오에서 당신은 "유령"을 봅니다.
    다른 세션이 삭제한 데이터가 내 세션에서는 여전히 보입니다!
    """)

    # 테스트용 임시 사용자 생성
    conn_setup = get_connection()
    cur_setup = conn_setup.cursor()
    cur_setup.execute("BEGIN")
    cur_setup.execute("DELETE FROM accounts WHERE name = 'Victim'")
    cur_setup.execute("""
        INSERT INTO accounts (name, balance)
        VALUES ('Victim', 7777)
    """)
    conn_setup.commit()
    cur_setup.close()
    conn_setup.close()

    conn_a = get_connection()  # 세션 A: REPEATABLE READ
    conn_b = get_connection()  # 세션 B: 삭제자

    cur_a = conn_a.cursor()
    cur_b = conn_b.cursor()

    try:
        # Step 1: 세션 A가 Victim 확인
        print("\n" + "─" * 70)
        print("  [Step 1] 세션 A: Victim 확인 (스냅샷 고정)")
        print("─" * 70)

        cur_a.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        cur_a.execute("SELECT xmin, xmax, id, name, balance FROM accounts WHERE name = 'Victim'")
        result = cur_a.fetchone()
        print(f"\n  세션 A가 본 데이터:")
        print(f"          xmin={result[0]}, xmax={result[1]}")
        print(f"          id={result[2]}, name='{result[3]}', balance={result[4]}")

        print_snapshot(cur_a, "세션 A")
        print("\n  └─ Victim이 존재합니다. 스냅샷이 고정되었습니다!")

        wait_for_user()

        # Step 2: 세션 B가 Victim 삭제
        print("\n" + "─" * 70)
        print("  [Step 2] 세션 B: Victim 삭제!")
        print("─" * 70)

        print("\n  세션 B: BEGIN")
        cur_b.execute("BEGIN")

        cur_b.execute("DELETE FROM accounts WHERE name = 'Victim'")
        print("  세션 B: DELETE 완료!")

        print("\n  세션 B: COMMIT")
        conn_b.commit()
        print("  세션 B: COMMIT 완료! Victim이 삭제되었습니다.")

        # 세션 B에서 확인 (새 트랜잭션)
        cur_b.execute("BEGIN")
        print_snapshot(cur_b, "세션 B")
        cur_b.execute("SELECT * FROM accounts WHERE name = 'Victim'")
        result_b = cur_b.fetchone()
        conn_b.commit()
        print(f"\n  세션 B에서 Victim 검색: {'있음' if result_b else '없음 (삭제됨)'}")

        wait_for_user()

        # Step 3: 세션 A에서 다시 확인 - 핵심!
        print("\n" + "─" * 70)
        print("  [Step 3] 세션 A: Victim 다시 확인 (핵심!)")
        print("─" * 70)

        print_snapshot(cur_a, "세션 A")
        print("  └─ 세션 A의 스냅샷은 여전히 고정된 상태!")

        cur_a.execute("SELECT xmin, xmax, id, name, balance FROM accounts WHERE name = 'Victim'")
        result_a = cur_a.fetchone()

        print(f"""
  ┌────────────────────────────────────────────────────────────────┐
  │                   👻 유령 삭제!                                │
  ├────────────────────────────────────────────────────────────────┤
  │                                                                │
  │   세션 B에서 Victim을 삭제하고 커밋했습니다.                    │
  │   그런데...                                                    │
  │                                                                │
  │   세션 A에서 Victim을 검색하면:                                 │
  │   → {'Victim이 아직 보입니다! 👻' if result_a else '없음'}                              │
  │                                                                │
  │   삭제됐는데 아직 보인다?!                                      │
  │   세션 A의 스냅샷에서는 xmax 트랜잭션이 아직 "미래"이거나        │
  │   진행 중으로 보이기 때문입니다.                                │
  │                                                                │
  └────────────────────────────────────────────────────────────────┘
        """)

        if result_a:
            print(f"  세션 A가 본 유령 데이터:")
            print(f"          xmin={result_a[0]}, xmax={result_a[1]}")
            print(f"          id={result_a[2]}, name='{result_a[3]}', balance={result_a[4]}")
            if result_a[1] != 0:
                print(f"\n  └─ xmax={result_a[1]}이 설정됨! 삭제 트랜잭션이 기록되었지만,")
                print(f"     세션 A의 스냅샷에서는 이 삭제가 '보이지 않음'")

        wait_for_user()

        # Step 4: 세션 A 커밋 후
        print("\n" + "─" * 70)
        print("  [Step 4] 세션 A: COMMIT 후 확인")
        print("─" * 70)

        conn_a.commit()
        print("\n  세션 A: COMMIT 완료!")

        cur_a.execute("BEGIN")
        print_snapshot(cur_a, "세션 A (새 트랜잭션)")
        cur_a.execute("SELECT * FROM accounts WHERE name = 'Victim'")
        result_final = cur_a.fetchone()
        conn_a.commit()
        print(f"\n  세션 A: Victim 검색 → {'있음' if result_final else '없음 (이제 삭제가 보임)'}")

        print_box([
            "핵심 교훈:",
            "- DELETE는 튜플의 xmax에 트랜잭션 ID를 기록합니다",
            "- 세션 A의 스냅샷에서는 xmax 트랜잭션이 '커밋됨'으로 안 보였습니다",
            "- 새 트랜잭션에서는 삭제가 '커밋됨'으로 보여서 튜플이 invisible",
        ])

    finally:
        # 정리 - Victim 삭제
        cur_b.execute("BEGIN")
        cur_b.execute("DELETE FROM accounts WHERE name = 'Victim'")
        conn_b.commit()
        cur_a.close()
        cur_b.close()
        conn_a.close()
        conn_b.close()


def scenario_3_time_traveler():
    """
    시나리오 3: 시간 여행자
    ----------------------
    실제 DB 값은 500인데, 내 세션에서는 1000이 보인다!
    """
    print_section("시나리오 3: 시간 여행자 (Time Traveler)")

    print("""
    이 시나리오에서 당신은 "시간 여행자"가 됩니다.
    다른 세션이 값을 변경해도, 당신은 과거의 값을 봅니다!
    """)

    # Alice 잔액 초기화
    conn_setup = get_connection()
    cur_setup = conn_setup.cursor()
    cur_setup.execute("BEGIN")
    cur_setup.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
    conn_setup.commit()
    cur_setup.close()
    conn_setup.close()

    conn_a = get_connection()  # 세션 A: 시간 여행자 (REPEATABLE READ)
    conn_b = get_connection()  # 세션 B: 현재 (변경자)

    cur_a = conn_a.cursor()
    cur_b = conn_b.cursor()

    try:
        # Step 1: 세션 A가 Alice 잔액 확인
        print("\n" + "─" * 70)
        print("  [Step 1] 세션 A: Alice 잔액 확인 (과거에 고정)")
        print("─" * 70)

        cur_a.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        cur_a.execute("SELECT xmin, xmax, balance FROM accounts WHERE name = 'Alice'")
        result = cur_a.fetchone()
        print(f"\n  세션 A가 본 Alice:")
        print(f"          xmin={result[0]}, xmax={result[1]}, balance={result[2]}원")

        print_snapshot(cur_a, "세션 A")
        print("\n  └─ 이 스냅샷이 트랜잭션이 끝날 때까지 고정됩니다!")

        wait_for_user()

        # Step 2: 세션 B가 Alice 잔액 변경
        print("\n" + "─" * 70)
        print("  [Step 2] 세션 B: Alice 잔액을 500원으로 변경")
        print("─" * 70)

        print("\n  세션 B: BEGIN")
        cur_b.execute("BEGIN")

        cur_b.execute("UPDATE accounts SET balance = 500 WHERE name = 'Alice'")
        print("  세션 B: UPDATE 완료!")
        print("          (UPDATE = 기존 튜플에 xmax 설정 + 새 튜플 생성)")

        print("\n  세션 B: COMMIT")
        conn_b.commit()
        print("  세션 B: COMMIT 완료!")

        # 세션 B에서 확인 (새 트랜잭션)
        cur_b.execute("BEGIN")
        print_snapshot(cur_b, "세션 B")
        cur_b.execute("SELECT xmin, xmax, balance FROM accounts WHERE name = 'Alice'")
        result_b = cur_b.fetchone()
        conn_b.commit()
        print(f"\n  세션 B가 본 Alice (새 튜플):")
        print(f"          xmin={result_b[0]}, xmax={result_b[1]}, balance={result_b[2]}원")

        wait_for_user()

        # Step 3: 동시 비교 - 핵심!
        print("\n" + "─" * 70)
        print("  [Step 3] 같은 순간, 다른 값! (핵심)")
        print("─" * 70)

        # 세션 A 스냅샷 확인 (여전히 고정)
        print_snapshot(cur_a, "세션 A")
        print("  └─ 세션 A의 스냅샷은 여전히 고정!")

        # 세션 A에서 다시 확인
        cur_a.execute("SELECT xmin, xmax, balance FROM accounts WHERE name = 'Alice'")
        result_a = cur_a.fetchone()

        # 세션 B에서 확인
        cur_b.execute("BEGIN")
        cur_b.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_b_now = cur_b.fetchone()[0]
        conn_b.commit()

        print(f"""
  ┌────────────────────────────────────────────────────────────────┐
  │                   ⏰ 시간 여행!                                │
  ├────────────────────────────────────────────────────────────────┤
  │                                                                │
  │   실제 DB의 Alice 잔액: {balance_b_now}원                           │
  │                                                                │
  │   그런데...                                                    │
  │                                                                │
  │   세션 A가 보는 Alice 잔액: {result_a[2]}원                        │
  │   세션 B가 보는 Alice 잔액: {balance_b_now}원                         │
  │                                                                │
  │   👆 같은 계좌인데 잔액이 다르게 보입니다!                      │
  │                                                                │
  │   세션 A가 보는 튜플: xmin={result_a[0]}, xmax={result_a[1]}              │
  │   (새 튜플의 xmin이 세션 A의 xmax보다 크므로 안 보임!)          │
  │                                                                │
  └────────────────────────────────────────────────────────────────┘
        """)

        wait_for_user()

        # Step 4: 세션 A 커밋 후
        print("\n" + "─" * 70)
        print("  [Step 4] 세션 A: COMMIT 후 확인 (현재로 복귀)")
        print("─" * 70)

        conn_a.commit()
        print("\n  세션 A: COMMIT 완료!")

        cur_a.execute("BEGIN")
        print_snapshot(cur_a, "세션 A (새 트랜잭션)")
        cur_a.execute("SELECT xmin, balance FROM accounts WHERE name = 'Alice'")
        result_final = cur_a.fetchone()
        conn_a.commit()
        print(f"\n  세션 A: Alice 잔액 = {result_final[1]}원 (xmin={result_final[0]})")
        print("  └─ 새 스냅샷이므로 새 튜플이 보입니다!")

        print_box([
            "핵심 교훈:",
            "- UPDATE는 기존 튜플의 xmax를 설정하고, 새 튜플을 생성합니다",
            "- 세션 A의 스냅샷에서는 새 튜플이 '미래'로 보여서 invisible",
            "- 세션 A는 xmax가 설정된 기존 튜플을 봅니다 (삭제가 안 보이므로)",
        ])

    finally:
        # 정리 - Alice 잔액 복구
        cur_b.execute("BEGIN")
        cur_b.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
        conn_b.commit()
        cur_a.close()
        cur_b.close()
        conn_a.close()
        conn_b.close()


def scenario_4_read_committed_vs_repeatable_read():
    """
    시나리오 4: READ COMMITTED vs REPEATABLE READ 비교
    ------------------------------------------------
    같은 상황에서 격리 수준에 따라 다른 결과!
    """
    print_section("시나리오 4: READ COMMITTED vs REPEATABLE READ")

    print("""
    이 시나리오에서 두 격리 수준의 차이를 명확하게 봅니다.
    같은 상황에서 READ COMMITTED와 REPEATABLE READ가 다르게 동작합니다!
    """)

    # Alice 잔액 초기화
    conn_setup = get_connection()
    cur_setup = conn_setup.cursor()
    cur_setup.execute("BEGIN")
    cur_setup.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
    conn_setup.commit()
    cur_setup.close()
    conn_setup.close()

    conn_rc = get_connection()  # READ COMMITTED
    conn_rr = get_connection()  # REPEATABLE READ
    conn_writer = get_connection()  # 변경자

    cur_rc = conn_rc.cursor()
    cur_rr = conn_rr.cursor()
    cur_writer = conn_writer.cursor()

    try:
        print("\n  두 세션을 동시에 시작합니다:")
        print("  - 세션 RC: READ COMMITTED (기본값)")
        print("  - 세션 RR: REPEATABLE READ")

        # 두 세션 시작
        cur_rc.execute("BEGIN")  # READ COMMITTED (기본값)
        cur_rr.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")

        # 첫 번째 SELECT
        print("\n" + "─" * 70)
        print("  [1차 SELECT] 변경 전")
        print("─" * 70)

        cur_rc.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_rc_1 = cur_rc.fetchone()[0]
        print_snapshot(cur_rc, "세션 RC")

        cur_rr.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_rr_1 = cur_rr.fetchone()[0]
        print_snapshot(cur_rr, "세션 RR")

        print(f"\n  세션 RC (READ COMMITTED):   {balance_rc_1}원")
        print(f"  세션 RR (REPEATABLE READ):  {balance_rr_1}원")
        print("  └─ 둘 다 1000원으로 같습니다 (아직 변경 전)")

        wait_for_user()

        # 다른 트랜잭션에서 변경
        print("\n" + "─" * 70)
        print("  [변경] 다른 세션에서 Alice 잔액을 500원으로 변경")
        print("─" * 70)

        print("\n  세션 Writer: BEGIN")
        cur_writer.execute("BEGIN")
        cur_writer.execute("UPDATE accounts SET balance = 500 WHERE name = 'Alice'")
        print("  세션 Writer: UPDATE 완료!")
        print("\n  세션 Writer: COMMIT")
        conn_writer.commit()
        print("  세션 Writer: COMMIT 완료!")

        # 변경 후 스냅샷 확인
        cur_writer.execute("BEGIN")
        print_snapshot(cur_writer, "세션 Writer (변경 후)")
        conn_writer.commit()

        wait_for_user()

        # 두 번째 SELECT - 핵심!
        print("\n" + "─" * 70)
        print("  [2차 SELECT] 변경 후 (핵심!)")
        print("─" * 70)

        # RC의 스냅샷 확인 (새로 생성됨)
        print("\n  READ COMMITTED는 매 SELECT마다 새 스냅샷:")
        print_snapshot(cur_rc, "세션 RC (2차)")

        cur_rc.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_rc_2 = cur_rc.fetchone()[0]

        # RR의 스냅샷 확인 (여전히 고정)
        print("\n  REPEATABLE READ는 스냅샷 고정:")
        print_snapshot(cur_rr, "세션 RR (2차)")

        cur_rr.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_rr_2 = cur_rr.fetchone()[0]

        print(f"""
  ┌────────────────────────────────────────────────────────────────┐
  │              격리 수준에 따른 차이!                            │
  ├────────────────────────────────────────────────────────────────┤
  │                                                                │
  │              1차 SELECT    변경 후    2차 SELECT               │
  │                                                                │
  │   READ COMMITTED:   {balance_rc_1}원    →  500원  →   {balance_rc_2}원      │
  │   REPEATABLE READ:  {balance_rr_1}원    →  500원  →   {balance_rr_2}원      │
  │                                                                │
  │   READ COMMITTED:  xmax가 증가 → 새 튜플이 보임                 │
  │   REPEATABLE READ: xmax가 고정 → 새 튜플의 xmin이 미래로 보임   │
  │                                                                │
  └────────────────────────────────────────────────────────────────┘
        """)

        conn_rc.rollback()
        conn_rr.rollback()

        print_box([
            "핵심 차이:",
            "- READ COMMITTED: 각 SELECT마다 새 스냅샷 (xmax 증가)",
            "- REPEATABLE READ: 트랜잭션 동안 스냅샷 고정 (xmax 불변)",
            "- 격리 수준 = 스냅샷 생성 시점의 차이!",
        ])

    finally:
        # 정리
        cur_writer.execute("BEGIN")
        cur_writer.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
        conn_writer.commit()
        cur_rc.close()
        cur_rr.close()
        cur_writer.close()
        conn_rc.close()
        conn_rr.close()
        conn_writer.close()


def scenario_5_concurrent_update_conflict():
    """
    시나리오 5: 동시 UPDATE 충돌 (First-Updater-Wins)
    ------------------------------------------------
    REPEATABLE READ에서 같은 row를 동시에 UPDATE하면?
    → 직렬화 오류(Serialization Failure) 발생!
    """
    print_section("시나리오 5: 동시 UPDATE 충돌 (First-Updater-Wins)")

    print("""
    이 시나리오에서 두 트랜잭션이 같은 row를 동시에 UPDATE합니다.
    REPEATABLE READ에서는 "먼저 UPDATE한 쪽이 승리"합니다.
    나중에 UPDATE한 쪽은 직렬화 오류를 받습니다!
    """)

    # Alice 잔액 초기화
    conn_setup = get_connection()
    cur_setup = conn_setup.cursor()
    cur_setup.execute("BEGIN")
    cur_setup.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
    conn_setup.commit()
    cur_setup.close()
    conn_setup.close()

    conn_a = get_connection()  # 세션 A: 먼저 UPDATE
    conn_b = get_connection()  # 세션 B: 나중에 UPDATE (실패할 예정)

    cur_a = conn_a.cursor()
    cur_b = conn_b.cursor()

    try:
        # Step 1: 두 세션 모두 REPEATABLE READ로 시작
        print("\n" + "─" * 70)
        print("  [Step 1] 두 세션 모두 REPEATABLE READ로 시작, 같은 row 조회")
        print("─" * 70)

        cur_a.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        cur_b.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")

        # 같은 row 조회
        cur_a.execute("SELECT xmin, balance FROM accounts WHERE name = 'Alice'")
        result_a = cur_a.fetchone()
        print(f"\n  세션 A가 본 Alice: xmin={result_a[0]}, balance={result_a[1]}원")
        print_snapshot(cur_a, "세션 A")

        cur_b.execute("SELECT xmin, balance FROM accounts WHERE name = 'Alice'")
        result_b = cur_b.fetchone()
        print(f"\n  세션 B가 본 Alice: xmin={result_b[0]}, balance={result_b[1]}원")
        print_snapshot(cur_b, "세션 B")

        print("\n  └─ 두 세션 모두 같은 스냅샷에서 같은 값(1000원)을 봅니다!")

        wait_for_user()

        # Step 2: 세션 A가 먼저 UPDATE
        print("\n" + "─" * 70)
        print("  [Step 2] 세션 A: 먼저 UPDATE (100원 출금)")
        print("─" * 70)

        print("\n  세션 A: UPDATE accounts SET balance = 900 WHERE name = 'Alice'")
        cur_a.execute("UPDATE accounts SET balance = 900 WHERE name = 'Alice'")
        print("  세션 A: UPDATE 성공! (row lock 획득)")

        # 세션 A가 본 값
        cur_a.execute("SELECT xmin, xmax, balance FROM accounts WHERE name = 'Alice'")
        result_a_after = cur_a.fetchone()
        print(f"\n  세션 A가 본 Alice (UPDATE 후):")
        print(f"          xmin={result_a_after[0]}, xmax={result_a_after[1]}, balance={result_a_after[2]}원")

        wait_for_user()

        # Step 3: 세션 B도 UPDATE 시도 (핵심!)
        print("\n" + "─" * 70)
        print("  [Step 3] 세션 B: UPDATE 시도 (200원 출금) - 핵심!")
        print("─" * 70)

        print("\n  세션 B: UPDATE accounts SET balance = 800 WHERE name = 'Alice'")
        print("  세션 B: ⏳ row lock 대기 중... (세션 A가 커밋할 때까지)")

        print(f"""
  ┌────────────────────────────────────────────────────────────────┐
  │                   🔒 Lock 상황                                 │
  ├────────────────────────────────────────────────────────────────┤
  │                                                                │
  │   세션 A: row lock 보유 중 (UPDATE 완료, 커밋 대기)            │
  │   세션 B: row lock 대기 중 (UPDATE 시도)                       │
  │                                                                │
  │   세션 B는 세션 A가 COMMIT 또는 ROLLBACK 할 때까지 대기합니다. │
  │                                                                │
  └────────────────────────────────────────────────────────────────┘
        """)

        wait_for_user()

        # Step 4: 세션 A 커밋 → 세션 B 오류 발생!
        print("\n" + "─" * 70)
        print("  [Step 4] 세션 A: COMMIT → 세션 B: 직렬화 오류!")
        print("─" * 70)

        print("\n  세션 A: COMMIT")
        conn_a.commit()
        print("  세션 A: COMMIT 완료!")

        # 세션 B UPDATE 시도 (여기서 오류 발생!)
        print("\n  세션 B: UPDATE 실행...")
        try:
            cur_b.execute("UPDATE accounts SET balance = 800 WHERE name = 'Alice'")
            conn_b.commit()
            print("  세션 B: UPDATE 성공?! (이 메시지는 보이면 안됨)")
        except psycopg2.errors.SerializationFailure as e:
            print(f"""
  ┌────────────────────────────────────────────────────────────────┐
  │                   💥 직렬화 오류 발생!                         │
  ├────────────────────────────────────────────────────────────────┤
  │                                                                │
  │   ERROR: could not serialize access due to concurrent update   │
  │                                                                │
  │   원인:                                                        │
  │   ─────                                                        │
  │   세션 B는 스냅샷에서 balance=1000을 봤습니다.                  │
  │   그런데 세션 A가 이미 그 row를 수정하고 커밋했습니다.          │
  │                                                                │
  │   세션 B가 계속 진행하면?                                       │
  │   → Lost Update! (세션 A의 변경이 사라짐)                      │
  │                                                                │
  │   PostgreSQL의 선택:                                           │
  │   → "안돼! 롤백하고 다시 시도해!" (직렬화 오류)                │
  │                                                                │
  └────────────────────────────────────────────────────────────────┘
            """)
            conn_b.rollback()

        wait_for_user()

        # Step 5: 최종 결과 확인
        print("\n" + "─" * 70)
        print("  [Step 5] 최종 결과 확인")
        print("─" * 70)

        cur_a.execute("BEGIN")
        cur_a.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        final_balance = cur_a.fetchone()[0]
        conn_a.commit()

        print(f"\n  최종 Alice 잔액: {final_balance}원")
        print(f"""
  ┌────────────────────────────────────────────────────────────────┐
  │                   결과 분석                                    │
  ├────────────────────────────────────────────────────────────────┤
  │                                                                │
  │   초기 잔액:     1000원                                        │
  │   세션 A 출금:   -100원 → 900원 (성공, 커밋됨)                  │
  │   세션 B 출금:   -200원 → 실패! (직렬화 오류)                   │
  │   최종 잔액:     {final_balance}원                                        │
  │                                                                │
  │   만약 READ COMMITTED였다면?                                   │
  │   → 세션 B가 새 값(900)을 읽고 UPDATE 진행                     │
  │   → 최종 잔액: 700원 (둘 다 성공)                              │
  │                                                                │
  │   REPEATABLE READ의 장점:                                      │
  │   → Lost Update 방지! (세션 B는 재시도 필요)                   │
  │                                                                │
  └────────────────────────────────────────────────────────────────┘
        """)

        print_box([
            "핵심 교훈:",
            "- REPEATABLE READ는 'First-Updater-Wins' 정책",
            "- 같은 row를 수정하려는 두 번째 트랜잭션은 직렬화 오류",
            "- 애플리케이션은 재시도 로직을 구현해야 함",
            "- READ COMMITTED는 오류 없이 진행 (다른 동작!)",
        ])

    finally:
        # 정리 - Alice 잔액 복구
        conn_cleanup = get_connection()
        cur_cleanup = conn_cleanup.cursor()
        cur_cleanup.execute("BEGIN")
        cur_cleanup.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
        conn_cleanup.commit()
        cur_cleanup.close()
        conn_cleanup.close()

        cur_a.close()
        cur_b.close()
        conn_a.close()
        conn_b.close()


def main():
    print("""
    ╔═══════════════════════════════════════════════════════════════════════╗
    ║                                                                       ║
    ║          Lab 00: 가시성 체험 - "같은 순간, 다른 현실"                  ║
    ║                                                                       ║
    ║   이 lab에서 당신은 MVCC의 핵심을 직접 체험합니다.                     ║
    ║   "커밋됐는데 왜 안 보여요?" 라는 질문의 답을 찾게 됩니다.             ║
    ║                                                                       ║
    ╚═══════════════════════════════════════════════════════════════════════╝
    """)

    try:
        scenario_1_parallel_universes()
        print("\n" + "═" * 70)

        scenario_2_ghost_delete()
        print("\n" + "═" * 70)

        scenario_3_time_traveler()
        print("\n" + "═" * 70)

        scenario_4_read_committed_vs_repeatable_read()
        print("\n" + "═" * 70)

        scenario_5_concurrent_update_conflict()

        print_section("Lab 00 완료!")
        print("""
    ┌───────────────────────────────────────────────────────────────────────┐
    │                         학습 정리                                     │
    ├───────────────────────────────────────────────────────────────────────┤
    │                                                                       │
    │   🌌 평행 우주: 같은 순간에 세션마다 다른 결과를 볼 수 있다            │
    │   👻 유령 삭제: 삭제된 데이터가 다른 스냅샷에서는 보인다               │
    │   ⏰ 시간 여행: 과거의 스냅샷에서는 과거의 값을 본다                   │
    │   💥 동시 충돌: REPEATABLE READ에서 동시 UPDATE는 직렬화 오류         │
    │                                                                       │
    │   핵심 개념:                                                          │
    │   ─────────                                                           │
    │   • MVCC = Multi-Version Concurrency Control                          │
    │   • 같은 데이터의 "여러 버전"이 동시에 존재                            │
    │   • 각 트랜잭션은 자신의 "스냅샷"을 봄                                 │
    │   • 격리 수준 = 스냅샷 생성 시점의 차이                                │
    │   • REPEATABLE READ는 "First-Updater-Wins" 정책                       │
    │                                                                       │
    │   다음 단계:                                                          │
    │   ─────────                                                           │
    │   • lab01_xmin_xmax.py - xmin, xmax 시스템 컬럼 이해                   │
    │   • lab02_update_delete.py - UPDATE/DELETE의 내부 동작                 │
    │   • lab02b_snapshot.py - 스냅샷 구조 상세 분석                         │
    │                                                                       │
    └───────────────────────────────────────────────────────────────────────┘
        """)

    except psycopg2.OperationalError as e:
        print(f"\n오류: 데이터베이스에 연결할 수 없습니다.")
        print(f"Docker가 실행 중인지 확인하세요: docker-compose up -d")
        print(f"상세 오류: {e}")


if __name__ == "__main__":
    main()
