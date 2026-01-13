"""
Lab 03: 트랜잭션 격리 수준 비교 실습
====================================

학습 목표:
- READ COMMITTED vs REPEATABLE READ 차이 체험
- Non-Repeatable Read 현상 직접 확인
- Write Skew (쓰기 치우침) 문제 이해

실행 방법:
    python lab03_isolation.py
"""

import psycopg2
from psycopg2 import extensions
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


def get_connection(autocommit=False, isolation_level=None):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = autocommit
    if isolation_level:
        conn.set_isolation_level(isolation_level)
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


def reset_alice_balance():
    """Alice의 잔액을 1000으로 리셋"""
    conn = get_connection(autocommit=True)
    cur = conn.cursor()
    cur.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")
    cur.close()
    conn.close()


def scenario_1_non_repeatable_read():
    """
    시나리오 1: Non-Repeatable Read (READ COMMITTED)
    ------------------------------------------------
    같은 트랜잭션 내에서 같은 쿼리가 다른 결과를 반환합니다.
    """
    print_section("시나리오 1: Non-Repeatable Read (READ COMMITTED)")

    reset_alice_balance()

    # 두 세션 준비
    conn_t1 = get_connection()  # T1: READ COMMITTED (기본값)
    conn_t2 = get_connection(autocommit=True)  # T2: 즉시 커밋

    cur_t1 = conn_t1.cursor()
    cur_t2 = conn_t2.cursor()

    try:
        print("\n--- READ COMMITTED에서 Non-Repeatable Read 발생 ---\n")

        # T1: 첫 번째 SELECT
        print("[T1] BEGIN (READ COMMITTED 기본값)")
        cur_t1.execute("BEGIN")
        cur_t1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_1 = cur_t1.fetchone()[0]
        print(f"[T1] 첫 번째 SELECT: Alice 잔액 = {balance_1}")

        # T2: Alice 잔액 변경
        print("\n[T2] Alice 잔액을 500으로 변경 & COMMIT")
        cur_t2.execute("UPDATE accounts SET balance = 500 WHERE name = 'Alice'")

        # T1: 두 번째 SELECT (같은 트랜잭션)
        cur_t1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_2 = cur_t1.fetchone()[0]
        print(f"\n[T1] 두 번째 SELECT: Alice 잔액 = {balance_2}")

        conn_t1.rollback()

        print(f"""
    결과 분석:
    - 첫 번째 SELECT: {balance_1}
    - 두 번째 SELECT: {balance_2}
    - 같은 트랜잭션인데 결과가 다름!

    이것이 'Non-Repeatable Read' 현상입니다.
    READ COMMITTED에서는 각 쿼리마다 새로운 스냅샷을 사용하기 때문에
    다른 트랜잭션의 커밋된 변경이 즉시 보입니다.
        """)

    finally:
        cur_t1.close()
        cur_t2.close()
        conn_t1.close()
        conn_t2.close()


def scenario_2_repeatable_read_consistency():
    """
    시나리오 2: REPEATABLE READ에서 일관성 유지
    ------------------------------------------
    같은 트랜잭션 내에서 항상 같은 결과를 봅니다.
    """
    print_section("시나리오 2: REPEATABLE READ에서 일관성 유지")

    reset_alice_balance()

    # T1: REPEATABLE READ
    conn_t1 = get_connection()
    conn_t2 = get_connection(autocommit=True)

    cur_t1 = conn_t1.cursor()
    cur_t2 = conn_t2.cursor()

    try:
        print("\n--- REPEATABLE READ에서 Non-Repeatable Read 방지 ---\n")

        # T1: REPEATABLE READ로 시작
        print("[T1] BEGIN ISOLATION LEVEL REPEATABLE READ")
        cur_t1.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        cur_t1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_1 = cur_t1.fetchone()[0]
        print(f"[T1] 첫 번째 SELECT: Alice 잔액 = {balance_1}")

        # T2: Alice 잔액 변경
        print("\n[T2] Alice 잔액을 500으로 변경 & COMMIT")
        cur_t2.execute("UPDATE accounts SET balance = 500 WHERE name = 'Alice'")

        # T1: 두 번째 SELECT
        cur_t1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_2 = cur_t1.fetchone()[0]
        print(f"\n[T1] 두 번째 SELECT: Alice 잔액 = {balance_2}")

        # T1: 실제 DB 값 확인 (트랜잭션 밖에서)
        conn_t1.rollback()
        cur_t1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        actual_balance = cur_t1.fetchone()[0]

        print(f"""
    결과 분석:
    - 첫 번째 SELECT: {balance_1}
    - 두 번째 SELECT: {balance_2}
    - 실제 DB 값: {actual_balance}

    REPEATABLE READ에서는 트랜잭션 시작 시점의 스냅샷을 고정합니다.
    다른 트랜잭션이 커밋하더라도 T1은 자신의 스냅샷을 계속 봅니다.
    → 일관된 읽기가 보장됨!
        """)

    finally:
        cur_t1.close()
        cur_t2.close()
        conn_t1.close()
        conn_t2.close()


def scenario_3_phantom_read():
    """
    시나리오 3: Phantom Read 테스트
    ------------------------------
    PostgreSQL의 REPEATABLE READ는 Phantom Read도 방지합니다.
    """
    print_section("시나리오 3: Phantom Read 테스트")

    # 초기화: 잔액 1000 이상인 계정 확인
    conn_setup = get_connection(autocommit=True)
    cur_setup = conn_setup.cursor()
    cur_setup.execute("DELETE FROM accounts WHERE name = 'Rich Guy'")
    cur_setup.close()
    conn_setup.close()

    conn_t1 = get_connection()
    conn_t2 = get_connection(autocommit=True)

    cur_t1 = conn_t1.cursor()
    cur_t2 = conn_t2.cursor()

    try:
        print("\n--- REPEATABLE READ에서 Phantom Read 방지 테스트 ---\n")

        # T1: REPEATABLE READ
        print("[T1] BEGIN ISOLATION LEVEL REPEATABLE READ")
        cur_t1.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")

        # 잔액 >= 1000인 계정 수 조회
        cur_t1.execute("SELECT COUNT(*) FROM accounts WHERE balance >= 1000")
        count_1 = cur_t1.fetchone()[0]
        print(f"[T1] 첫 번째 COUNT (balance >= 1000): {count_1}개")

        # T2: 새 계정 추가 (잔액 5000)
        print("\n[T2] 새 계정 'Rich Guy' 추가 (잔액 5000) & COMMIT")
        cur_t2.execute("""
            INSERT INTO accounts (name, balance) VALUES ('Rich Guy', 5000)
        """)

        # T1: 같은 조건으로 다시 조회
        cur_t1.execute("SELECT COUNT(*) FROM accounts WHERE balance >= 1000")
        count_2 = cur_t1.fetchone()[0]
        print(f"\n[T1] 두 번째 COUNT (balance >= 1000): {count_2}개")

        conn_t1.rollback()

        # 실제 개수 확인
        cur_t2.execute("SELECT COUNT(*) FROM accounts WHERE balance >= 1000")
        actual_count = cur_t2.fetchone()[0]

        print(f"""
    결과 분석:
    - 첫 번째 COUNT: {count_1}개
    - 두 번째 COUNT: {count_2}개
    - 실제 DB 값: {actual_count}개

    PostgreSQL의 REPEATABLE READ는 표준 SQL의 REPEATABLE READ보다 강력합니다.
    'Snapshot Isolation'을 구현하여 Phantom Read도 방지합니다.
    (참고: 다른 DBMS에서는 SERIALIZABLE이 필요할 수 있음)
        """)

    finally:
        cur_t2.execute("DELETE FROM accounts WHERE name = 'Rich Guy'")
        cur_t1.close()
        cur_t2.close()
        conn_t1.close()
        conn_t2.close()


def scenario_4_write_skew():
    """
    시나리오 4: Write Skew (쓰기 치우침)
    -----------------------------------
    REPEATABLE READ로도 방지할 수 없는 이상 현상입니다.
    """
    print_section("시나리오 4: Write Skew (의사 당직 예제)")

    # 초기화: 두 의사 모두 당직 중
    conn_setup = get_connection(autocommit=True)
    cur_setup = conn_setup.cursor()
    cur_setup.execute("""
        UPDATE doctors_on_call
        SET is_on_call = true
        WHERE shift_date = CURRENT_DATE
    """)
    cur_setup.close()
    conn_setup.close()

    conn_kim = get_connection()  # Dr. Kim
    conn_lee = get_connection()  # Dr. Lee

    cur_kim = conn_kim.cursor()
    cur_lee = conn_lee.cursor()

    try:
        print("""
    시나리오: 병원 규칙 - 최소 1명은 항상 당직 중이어야 함
    현재 상태: Dr. Kim과 Dr. Lee 모두 당직 중
    문제 상황: 두 의사가 동시에 당직 해제를 시도
        """)

        # 두 트랜잭션 시작 (REPEATABLE READ)
        print("[Dr. Kim] BEGIN ISOLATION LEVEL REPEATABLE READ")
        cur_kim.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        print("[Dr. Lee] BEGIN ISOLATION LEVEL REPEATABLE READ")
        cur_lee.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")

        # 두 의사 모두 "다른 의사가 당직 중인지" 확인
        cur_kim.execute("""
            SELECT COUNT(*) FROM doctors_on_call
            WHERE shift_date = CURRENT_DATE AND is_on_call = true
            AND doctor_name != 'Dr. Kim'
        """)
        others_for_kim = cur_kim.fetchone()[0]
        print(f"\n[Dr. Kim] 다른 당직 의사 수: {others_for_kim}명 → '해제해도 되겠군!'")

        cur_lee.execute("""
            SELECT COUNT(*) FROM doctors_on_call
            WHERE shift_date = CURRENT_DATE AND is_on_call = true
            AND doctor_name != 'Dr. Lee'
        """)
        others_for_lee = cur_lee.fetchone()[0]
        print(f"[Dr. Lee] 다른 당직 의사 수: {others_for_lee}명 → '해제해도 되겠군!'")

        # 두 의사 모두 당직 해제
        print("\n[Dr. Kim] 당직 해제!")
        cur_kim.execute("""
            UPDATE doctors_on_call
            SET is_on_call = false
            WHERE doctor_name = 'Dr. Kim' AND shift_date = CURRENT_DATE
        """)
        conn_kim.commit()

        print("[Dr. Lee] 당직 해제!")
        cur_lee.execute("""
            UPDATE doctors_on_call
            SET is_on_call = false
            WHERE doctor_name = 'Dr. Lee' AND shift_date = CURRENT_DATE
        """)
        conn_lee.commit()

        # 결과 확인
        cur_kim.execute("""
            SELECT doctor_name, is_on_call
            FROM doctors_on_call
            WHERE shift_date = CURRENT_DATE
        """)
        print("\n[결과 확인]")
        print_result(cur_kim, "현재 당직 상태")

        cur_kim.execute("""
            SELECT COUNT(*) FROM doctors_on_call
            WHERE shift_date = CURRENT_DATE AND is_on_call = true
        """)
        on_call_count = cur_kim.fetchone()[0]

        print(f"""
    분석:
    - 당직 중인 의사 수: {on_call_count}명
    - 병원 규칙 위반: {'예 (모두 당직 해제!)' if on_call_count == 0 else '아니오'}

    이것이 'Write Skew' 현상입니다:
    1. 두 트랜잭션이 각각 "다른 의사가 있다"는 것을 확인
    2. 서로 다른 row를 수정하므로 충돌 없이 커밋 성공
    3. 결과적으로 불변식(최소 1명 당직) 위반

    해결책: SERIALIZABLE 격리 수준 사용
        """)

    finally:
        # 원상복구
        conn_setup = get_connection(autocommit=True)
        cur_setup = conn_setup.cursor()
        cur_setup.execute("""
            UPDATE doctors_on_call SET is_on_call = true
            WHERE shift_date = CURRENT_DATE
        """)
        cur_setup.close()
        conn_setup.close()

        cur_kim.close()
        cur_lee.close()
        conn_kim.close()
        conn_lee.close()


def scenario_5_serializable_prevents_write_skew():
    """
    시나리오 5: SERIALIZABLE로 Write Skew 방지
    -----------------------------------------
    SERIALIZABLE은 Write Skew를 감지하고 방지합니다.
    """
    print_section("시나리오 5: SERIALIZABLE로 Write Skew 방지")

    # 초기화
    conn_setup = get_connection(autocommit=True)
    cur_setup = conn_setup.cursor()
    cur_setup.execute("""
        UPDATE doctors_on_call SET is_on_call = true
        WHERE shift_date = CURRENT_DATE
    """)
    cur_setup.close()
    conn_setup.close()

    conn_kim = get_connection()
    conn_lee = get_connection()

    cur_kim = conn_kim.cursor()
    cur_lee = conn_lee.cursor()

    try:
        print("\n--- SERIALIZABLE 격리 수준으로 Write Skew 방지 ---\n")

        # SERIALIZABLE로 시작
        print("[Dr. Kim] BEGIN ISOLATION LEVEL SERIALIZABLE")
        cur_kim.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        print("[Dr. Lee] BEGIN ISOLATION LEVEL SERIALIZABLE")
        cur_lee.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")

        # 두 의사 모두 확인
        cur_kim.execute("""
            SELECT COUNT(*) FROM doctors_on_call
            WHERE shift_date = CURRENT_DATE AND is_on_call = true
            AND doctor_name != 'Dr. Kim'
        """)
        cur_kim.fetchone()
        print("[Dr. Kim] 다른 당직 의사 확인 완료")

        cur_lee.execute("""
            SELECT COUNT(*) FROM doctors_on_call
            WHERE shift_date = CURRENT_DATE AND is_on_call = true
            AND doctor_name != 'Dr. Lee'
        """)
        cur_lee.fetchone()
        print("[Dr. Lee] 다른 당직 의사 확인 완료")

        # 당직 해제 시도
        print("\n[Dr. Kim] 당직 해제 시도...")
        cur_kim.execute("""
            UPDATE doctors_on_call SET is_on_call = false
            WHERE doctor_name = 'Dr. Kim' AND shift_date = CURRENT_DATE
        """)
        conn_kim.commit()
        print("[Dr. Kim] 커밋 성공!")

        print("[Dr. Lee] 당직 해제 시도...")
        try:
            cur_lee.execute("""
                UPDATE doctors_on_call SET is_on_call = false
                WHERE doctor_name = 'Dr. Lee' AND shift_date = CURRENT_DATE
            """)
            conn_lee.commit()
            print("[Dr. Lee] 커밋 성공!")
        except psycopg2.errors.SerializationFailure as e:
            print(f"[Dr. Lee] 커밋 실패! 직렬화 오류 발생")
            print(f"          오류 메시지: could not serialize access...")
            conn_lee.rollback()

        # 결과 확인
        cur_kim.execute("""
            SELECT doctor_name, is_on_call
            FROM doctors_on_call
            WHERE shift_date = CURRENT_DATE
        """)
        print("\n[결과 확인]")
        print_result(cur_kim, "현재 당직 상태")

        print("""
    SERIALIZABLE이 Write Skew를 방지했습니다!
    - PostgreSQL은 SSI (Serializable Snapshot Isolation)를 사용
    - 트랜잭션 간 의존성을 추적하여 직렬화 불가능한 경우 abort
    - 애플리케이션은 재시도 로직을 구현해야 함
        """)

    finally:
        conn_setup = get_connection(autocommit=True)
        cur_setup = conn_setup.cursor()
        cur_setup.execute("""
            UPDATE doctors_on_call SET is_on_call = true
            WHERE shift_date = CURRENT_DATE
        """)
        cur_setup.close()
        conn_setup.close()

        cur_kim.close()
        cur_lee.close()
        conn_kim.close()
        conn_lee.close()


def main():
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║        Lab 03: 트랜잭션 격리 수준 비교 실습                  ║
    ║                                                           ║
    ║  각 격리 수준의 특성과 이상 현상을 직접 체험합니다.            ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    try:
        scenario_1_non_repeatable_read()
        scenario_2_repeatable_read_consistency()
        scenario_3_phantom_read()
        scenario_4_write_skew()
        scenario_5_serializable_prevents_write_skew()

        print_section("Lab 03 완료!")
        print("""
    학습 정리:

    | 격리 수준         | Dirty Read | Non-Rep Read | Phantom | Write Skew |
    |-------------------|------------|--------------|---------|------------|
    | READ COMMITTED    | X          | O            | O       | O          |
    | REPEATABLE READ   | X          | X            | X*      | O          |
    | SERIALIZABLE      | X          | X            | X       | X          |

    * PostgreSQL의 REPEATABLE READ는 Snapshot Isolation으로
      표준보다 강력하게 Phantom Read도 방지

    선택 가이드:
    - 대부분의 경우: READ COMMITTED (기본값)
    - 읽기 일관성 필요: REPEATABLE READ
    - 금융/재고 등 엄격한 정합성: SERIALIZABLE (+ 재시도 로직)

    다음 실습: lab04_concurrent.py
        """)

    except psycopg2.OperationalError as e:
        print(f"\n오류: 데이터베이스에 연결할 수 없습니다.")
        print(f"Docker가 실행 중인지 확인하세요: docker-compose up -d")
        print(f"상세 오류: {e}")


if __name__ == "__main__":
    main()
