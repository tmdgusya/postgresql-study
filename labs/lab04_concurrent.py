"""
Lab 04: 동시 쓰기 시나리오 실습
==============================

학습 목표:
- 동시에 같은 row를 수정할 때 PostgreSQL의 동작 이해
- Lost Update 방지 메커니즘 확인
- REPEATABLE READ에서의 충돌 처리

실행 방법:
    python lab04_concurrent.py
"""

import psycopg2
from psycopg2 import errors
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


def reset_alice_balance(amount=1000):
    conn = get_connection(autocommit=True)
    cur = conn.cursor()
    cur.execute("UPDATE accounts SET balance = %s WHERE name = 'Alice'", (amount,))
    cur.close()
    conn.close()


def scenario_1_row_level_lock():
    """
    시나리오 1: Row-Level Lock으로 Lost Update 방지
    -----------------------------------------------
    두 트랜잭션이 같은 row를 동시에 UPDATE하려 할 때
    """
    print_section("시나리오 1: Row-Level Lock으로 Lost Update 방지")

    reset_alice_balance(1000)

    results = {'t1': None, 't2': None, 't1_time': 0, 't2_time': 0}
    barrier = threading.Barrier(2)

    def transaction_1():
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            print("[T1] BEGIN")

            barrier.wait()  # T2와 동시 시작

            start = time.time()
            print("[T1] UPDATE accounts SET balance = balance - 100 WHERE name = 'Alice'")
            cur.execute("UPDATE accounts SET balance = balance - 100 WHERE name = 'Alice'")
            print("[T1] UPDATE 완료! (락 획득)")

            time.sleep(2)  # 락 유지

            conn.commit()
            print("[T1] COMMIT")
            results['t1_time'] = time.time() - start

            cur.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
            results['t1'] = cur.fetchone()[0]

        finally:
            cur.close()
            conn.close()

    def transaction_2():
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            print("[T2] BEGIN")

            barrier.wait()  # T1과 동시 시작
            time.sleep(0.1)  # T1이 먼저 락 획득하도록

            start = time.time()
            print("[T2] UPDATE accounts SET balance = balance - 50 WHERE name = 'Alice'")
            print("[T2] ... 대기 중 (T1이 락 보유)")
            cur.execute("UPDATE accounts SET balance = balance - 50 WHERE name = 'Alice'")
            results['t2_time'] = time.time() - start
            print(f"[T2] UPDATE 완료! (대기 시간: {results['t2_time']:.2f}초)")

            conn.commit()
            print("[T2] COMMIT")

            cur.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
            results['t2'] = cur.fetchone()[0]

        finally:
            cur.close()
            conn.close()

    print("""
    초기 상태: Alice 잔액 = 1000
    T1: balance - 100 (먼저 실행)
    T2: balance - 50 (T1 대기 후 실행)
    기대 결과: 1000 - 100 - 50 = 850
    """)

    t1 = threading.Thread(target=transaction_1)
    t2 = threading.Thread(target=transaction_2)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    print(f"""
    결과 분석:
    - T1 완료 후 잔액: {results['t1']} (1000 - 100 = 900)
    - T2 완료 후 잔액: {results['t2']} (900 - 50 = 850)
    - T2 대기 시간: {results['t2_time']:.2f}초

    핵심 포인트:
    1. PostgreSQL은 row-level lock으로 동시 UPDATE 제어
    2. T2는 T1이 커밋할 때까지 대기
    3. T1 커밋 후 T2는 '최신 값'을 기준으로 계산
    4. Lost Update가 발생하지 않음!
    """)


def scenario_2_repeatable_read_conflict():
    """
    시나리오 2: REPEATABLE READ에서 UPDATE 충돌
    -------------------------------------------
    스냅샷을 읽은 후 다른 트랜잭션이 수정하면 충돌 발생
    """
    print_section("시나리오 2: REPEATABLE READ에서 UPDATE 충돌")

    reset_alice_balance(1000)

    conn_t1 = get_connection()
    conn_t2 = get_connection(autocommit=True)

    cur_t1 = conn_t1.cursor()
    cur_t2 = conn_t2.cursor()

    try:
        print("""
    시나리오:
    T1: REPEATABLE READ로 Alice 잔액 읽음
    T2: Alice 잔액 수정 & 커밋
    T1: Alice 잔액 UPDATE 시도 → 충돌!
        """)

        # T1: REPEATABLE READ 시작
        print("[T1] BEGIN ISOLATION LEVEL REPEATABLE READ")
        cur_t1.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")

        # T1: 잔액 읽기 (스냅샷 생성)
        cur_t1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_t1 = cur_t1.fetchone()[0]
        print(f"[T1] SELECT balance: {balance_t1}")

        # T2: 잔액 수정
        print("\n[T2] UPDATE accounts SET balance = 0 WHERE name = 'Alice'")
        cur_t2.execute("UPDATE accounts SET balance = 0 WHERE name = 'Alice'")
        print("[T2] COMMIT (암시적)")

        # T1: UPDATE 시도
        print("\n[T1] UPDATE accounts SET balance = balance + 100 WHERE name = 'Alice'")
        try:
            cur_t1.execute("UPDATE accounts SET balance = balance + 100 WHERE name = 'Alice'")
            conn_t1.commit()
            print("[T1] 성공?!")
        except errors.SerializationFailure as e:
            print("[T1] 오류 발생!")
            print("     ERROR: could not serialize access due to concurrent update")
            conn_t1.rollback()

        print("""
    분석:
    - T1은 balance = 1000인 스냅샷을 가지고 있음
    - T2가 balance = 0으로 변경 후 커밋
    - T1이 UPDATE 시도 시 "내가 본 row가 변경됨" 감지
    - REPEATABLE READ는 이런 경우 오류 발생!

    해결책:
    1. 애플리케이션에서 재시도 로직 구현
    2. 또는 SELECT FOR UPDATE로 미리 락 획득
        """)

    finally:
        reset_alice_balance(1000)
        cur_t1.close()
        cur_t2.close()
        conn_t1.close()
        conn_t2.close()


def scenario_3_select_for_update():
    """
    시나리오 3: SELECT FOR UPDATE로 미리 락 획득
    --------------------------------------------
    읽는 시점에 락을 획득하여 충돌 방지
    """
    print_section("시나리오 3: SELECT FOR UPDATE로 미리 락 획득")

    reset_alice_balance(1000)

    results = {'t1_balance': None, 't2_wait': 0}
    barrier = threading.Barrier(2)

    def transaction_1():
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            print("[T1] BEGIN")

            barrier.wait()

            print("[T1] SELECT balance FROM accounts WHERE name = 'Alice' FOR UPDATE")
            cur.execute("SELECT balance FROM accounts WHERE name = 'Alice' FOR UPDATE")
            balance = cur.fetchone()[0]
            print(f"[T1] 잔액: {balance} (락 획득)")

            time.sleep(2)  # 락 유지

            cur.execute("UPDATE accounts SET balance = balance - 100 WHERE name = 'Alice'")
            conn.commit()
            print("[T1] UPDATE & COMMIT 완료")

        finally:
            cur.close()
            conn.close()

    def transaction_2():
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            print("[T2] BEGIN")

            barrier.wait()
            time.sleep(0.1)  # T1이 먼저 실행되도록

            start = time.time()
            print("[T2] SELECT balance FROM accounts WHERE name = 'Alice' FOR UPDATE")
            print("[T2] ... 대기 중 (T1이 락 보유)")
            cur.execute("SELECT balance FROM accounts WHERE name = 'Alice' FOR UPDATE")
            results['t2_wait'] = time.time() - start
            balance = cur.fetchone()[0]
            print(f"[T2] 잔액: {balance} (락 획득, 대기: {results['t2_wait']:.2f}초)")

            cur.execute("UPDATE accounts SET balance = balance - 50 WHERE name = 'Alice'")
            conn.commit()
            print("[T2] UPDATE & COMMIT 완료")

            cur.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
            results['t1_balance'] = cur.fetchone()[0]

        finally:
            cur.close()
            conn.close()

    print("""
    SELECT FOR UPDATE 사용 시:
    - SELECT 시점에 row-level exclusive lock 획득
    - 다른 트랜잭션은 같은 row를 수정하거나 FOR UPDATE로 읽을 수 없음
    - "읽고 → 로직 수행 → 쓰기" 패턴에서 안전하게 사용 가능
    """)

    t1 = threading.Thread(target=transaction_1)
    t2 = threading.Thread(target=transaction_2)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    print(f"""
    결과:
    - 최종 잔액: {results['t1_balance']}
    - T2 대기 시간: {results['t2_wait']:.2f}초
    - SELECT FOR UPDATE로 안전하게 순차 처리됨
    """)


def scenario_4_concurrent_inserts():
    """
    시나리오 4: 동시 INSERT와 UNIQUE 제약
    ------------------------------------
    같은 unique 값을 동시에 INSERT하려 할 때
    """
    print_section("시나리오 4: 동시 INSERT와 UNIQUE 제약")

    conn_setup = get_connection(autocommit=True)
    cur_setup = conn_setup.cursor()
    cur_setup.execute("DELETE FROM accounts WHERE name = 'Unique Test'")
    cur_setup.close()
    conn_setup.close()

    results = {'t1': None, 't2': None}
    barrier = threading.Barrier(2)

    def transaction_1():
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            barrier.wait()

            print("[T1] INSERT INTO accounts (name, balance) VALUES ('Unique Test', 100)")
            cur.execute("INSERT INTO accounts (name, balance) VALUES ('Unique Test', 100)")
            time.sleep(1)
            conn.commit()
            print("[T1] COMMIT 성공!")
            results['t1'] = 'success'

        except Exception as e:
            print(f"[T1] 오류: {type(e).__name__}")
            results['t1'] = 'error'
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def transaction_2():
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            barrier.wait()
            time.sleep(0.1)  # T1이 먼저

            print("[T2] INSERT INTO accounts (name, balance) VALUES ('Unique Test', 200)")
            print("[T2] ... 대기 중")
            cur.execute("INSERT INTO accounts (name, balance) VALUES ('Unique Test', 200)")
            conn.commit()
            print("[T2] COMMIT 성공!")
            results['t2'] = 'success'

        except errors.UniqueViolation as e:
            print("[T2] UniqueViolation 오류!")
            results['t2'] = 'unique_violation'
            conn.rollback()
        except Exception as e:
            print(f"[T2] 오류: {type(e).__name__}")
            results['t2'] = 'error'
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    print("""
    name 컬럼에 UNIQUE 제약이 없지만, 같은 값을 INSERT하는 경우를 시뮬레이션합니다.
    (실제로는 INSERT 자체는 성공하지만 개념 설명용)

    UNIQUE 제약이 있다면:
    - T1이 먼저 INSERT하고 아직 커밋 안함
    - T2가 같은 값 INSERT 시도 → T1 커밋 대기
    - T1 커밋 → T2 UniqueViolation 발생
    """)

    t1 = threading.Thread(target=transaction_1)
    t2 = threading.Thread(target=transaction_2)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # 정리
    conn_setup = get_connection(autocommit=True)
    cur_setup = conn_setup.cursor()
    cur_setup.execute("DELETE FROM accounts WHERE name = 'Unique Test'")
    cur_setup.close()
    conn_setup.close()


def scenario_5_optimistic_locking():
    """
    시나리오 5: Optimistic Locking 패턴
    ----------------------------------
    애플리케이션 레벨에서 버전 체크로 동시성 제어
    """
    print_section("시나리오 5: Optimistic Locking 패턴")

    # 테스트 테이블 생성
    conn = get_connection(autocommit=True)
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS products_versioned;
        CREATE TABLE products_versioned (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            price INTEGER,
            version INTEGER DEFAULT 1
        );
        INSERT INTO products_versioned (name, price) VALUES ('Widget', 100);
    """)
    cur.close()
    conn.close()

    def update_with_version(conn, product_id, new_price, expected_version):
        """버전 체크 후 UPDATE (Optimistic Locking)"""
        cur = conn.cursor()
        cur.execute("""
            UPDATE products_versioned
            SET price = %s, version = version + 1
            WHERE id = %s AND version = %s
        """, (new_price, product_id, expected_version))
        affected = cur.rowcount
        cur.close()
        return affected > 0

    try:
        print("""
    Optimistic Locking:
    - 읽을 때 버전 번호도 함께 읽음
    - UPDATE 시 "내가 읽은 버전이 아직 유효한지" 체크
    - 버전이 변경되었으면 UPDATE 실패 → 재시도

    장점: 락을 잡지 않아 높은 동시성
    단점: 충돌 시 재시도 필요
        """)

        # 두 트랜잭션이 같은 버전을 읽음
        conn_t1 = get_connection()
        conn_t2 = get_connection()
        cur_t1 = conn_t1.cursor()
        cur_t2 = conn_t2.cursor()

        cur_t1.execute("SELECT id, price, version FROM products_versioned WHERE id = 1")
        row_t1 = cur_t1.fetchone()
        print(f"[T1] 읽음: id={row_t1[0]}, price={row_t1[1]}, version={row_t1[2]}")

        cur_t2.execute("SELECT id, price, version FROM products_versioned WHERE id = 1")
        row_t2 = cur_t2.fetchone()
        print(f"[T2] 읽음: id={row_t2[0]}, price={row_t2[1]}, version={row_t2[2]}")

        # T1 먼저 UPDATE
        print("\n[T1] UPDATE price=150 WHERE version=1")
        success = update_with_version(conn_t1, 1, 150, row_t1[2])
        conn_t1.commit()
        print(f"[T1] 결과: {'성공' if success else '실패'}")

        # T2 UPDATE 시도 (같은 버전으로)
        print("\n[T2] UPDATE price=200 WHERE version=1")
        success = update_with_version(conn_t2, 1, 200, row_t2[2])
        conn_t2.commit()
        print(f"[T2] 결과: {'성공' if success else '실패 (버전 불일치!)'}")

        # 최종 상태
        cur_t1.execute("SELECT * FROM products_versioned WHERE id = 1")
        final = cur_t1.fetchone()
        print(f"\n최종 상태: price={final[2]}, version={final[3]}")

        print("""
    T2는 버전이 이미 2로 변경되어 UPDATE가 실패했습니다.
    애플리케이션은 이를 감지하고:
    1. 최신 데이터를 다시 읽기
    2. 비즈니스 로직 재수행
    3. 다시 UPDATE 시도
        """)

        cur_t1.close()
        cur_t2.close()
        conn_t1.close()
        conn_t2.close()

    finally:
        conn = get_connection(autocommit=True)
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS products_versioned")
        cur.close()
        conn.close()


def main():
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║           Lab 04: 동시 쓰기 시나리오 실습                    ║
    ║                                                           ║
    ║  동시에 같은 데이터를 수정할 때 PostgreSQL의 동작을 학습합니다. ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    try:
        scenario_1_row_level_lock()
        scenario_2_repeatable_read_conflict()
        scenario_3_select_for_update()
        scenario_4_concurrent_inserts()
        scenario_5_optimistic_locking()

        print_section("Lab 04 완료!")
        print("""
    학습 정리:

    1. Row-Level Lock:
       - UPDATE 시 자동으로 row lock 획득
       - 다른 트랜잭션은 커밋될 때까지 대기
       - Lost Update 방지

    2. REPEATABLE READ 충돌:
       - 읽은 후 다른 트랜잭션이 수정하면 오류
       - 재시도 로직 필요

    3. SELECT FOR UPDATE:
       - 읽는 시점에 미리 락 획득
       - "읽고-계산-쓰기" 패턴에 적합

    4. Optimistic Locking:
       - 버전 번호로 충돌 감지
       - 높은 동시성, 충돌 시 재시도

    다음 실습: lab05_vacuum.py
        """)

    except psycopg2.OperationalError as e:
        print(f"\n오류: 데이터베이스에 연결할 수 없습니다.")
        print(f"Docker가 실행 중인지 확인하세요: docker-compose up -d")
        print(f"상세 오류: {e}")


if __name__ == "__main__":
    main()
