"""
Lab 06: Lock 모니터링 실습
=========================

학습 목표:
- PostgreSQL의 다양한 Lock 유형 이해
- pg_locks로 현재 Lock 상태 모니터링
- Lock 대기 상황 분석
- Deadlock 발생시키고 해결 과정 관찰

실행 방법:
    python lab06_locks.py
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


def scenario_1_lock_types():
    """
    시나리오 1: PostgreSQL Lock 유형 이해
    ------------------------------------
    다양한 Lock 유형과 충돌 관계를 설명합니다.
    """
    print_section("시나리오 1: PostgreSQL Lock 유형")

    print("""
    PostgreSQL Lock 유형 (호환성 낮은 순):

    ┌─────────────────────────────────────────────────────────────┐
    │ ACCESS SHARE (SELECT)                                       │
    │   - 가장 약한 락                                             │
    │   - 다른 모든 락과 호환 (ACCESS EXCLUSIVE 제외)              │
    ├─────────────────────────────────────────────────────────────┤
    │ ROW SHARE (SELECT FOR UPDATE/SHARE)                         │
    │   - row 레벨 락 획득 의도                                    │
    ├─────────────────────────────────────────────────────────────┤
    │ ROW EXCLUSIVE (INSERT, UPDATE, DELETE)                      │
    │   - 데이터 수정 시 자동 획득                                 │
    ├─────────────────────────────────────────────────────────────┤
    │ SHARE UPDATE EXCLUSIVE (VACUUM, CREATE INDEX CONCURRENTLY)  │
    │   - 자기 자신과 충돌                                         │
    ├─────────────────────────────────────────────────────────────┤
    │ SHARE (CREATE INDEX)                                        │
    │   - 읽기만 허용, 쓰기 차단                                   │
    ├─────────────────────────────────────────────────────────────┤
    │ SHARE ROW EXCLUSIVE                                         │
    │   - 거의 사용 안 함                                          │
    ├─────────────────────────────────────────────────────────────┤
    │ EXCLUSIVE                                                   │
    │   - ACCESS SHARE만 호환                                      │
    ├─────────────────────────────────────────────────────────────┤
    │ ACCESS EXCLUSIVE (DROP, ALTER, VACUUM FULL, LOCK TABLE)     │
    │   - 가장 강한 락, 모든 락과 충돌                             │
    │   - 테이블에 어떤 접근도 불가                                │
    └─────────────────────────────────────────────────────────────┘

    Lock 충돌 매트릭스:
    https://www.postgresql.org/docs/current/explicit-locking.html
    """)


def scenario_2_row_level_lock():
    """
    시나리오 2: Row-Level Lock 관찰
    ------------------------------
    SELECT FOR UPDATE가 어떤 락을 거는지 확인합니다.
    """
    print_section("시나리오 2: Row-Level Lock 관찰")

    conn_holder = get_connection()  # 락 보유자
    conn_monitor = get_connection(autocommit=True)  # 모니터링

    cur_holder = conn_holder.cursor()
    cur_monitor = conn_monitor.cursor()

    try:
        # 락 획득
        print("\n[세션 1] SELECT FOR UPDATE로 row 락 획득")
        cur_holder.execute("BEGIN")
        cur_holder.execute("""
            SELECT * FROM accounts WHERE name = 'Alice' FOR UPDATE
        """)
        print("[세션 1] Alice row에 락 획득 완료")

        # 현재 락 상태 확인
        cur_monitor.execute("""
            SELECT
                l.locktype,
                l.relation::regclass as table_name,
                l.mode,
                l.granted,
                a.usename,
                a.state,
                LEFT(a.query, 50) as query
            FROM pg_locks l
            JOIN pg_stat_activity a ON l.pid = a.pid
            WHERE l.relation = 'accounts'::regclass
            AND a.pid != pg_backend_pid()
        """)
        print_result(cur_monitor, "현재 accounts 테이블 락 상태")

        # tuple 레벨 락 확인
        cur_monitor.execute("""
            SELECT
                l.locktype,
                l.page,
                l.tuple,
                l.mode,
                l.granted
            FROM pg_locks l
            WHERE l.locktype = 'tuple'
        """)
        result = print_result(cur_monitor, "Tuple 레벨 락")

        print("""
    분석:
    - 테이블 레벨: RowShareLock (SELECT FOR UPDATE 의도 표시)
    - 튜플 레벨: 실제 row에 대한 락 (ExclusiveLock)

    FOR UPDATE vs FOR SHARE:
    - FOR UPDATE: 수정 의도, ExclusiveLock
    - FOR SHARE: 읽기만, ShareLock (다른 FOR SHARE와 호환)
        """)

        conn_holder.rollback()

    finally:
        cur_holder.close()
        cur_monitor.close()
        conn_holder.close()
        conn_monitor.close()


def scenario_3_lock_waiting():
    """
    시나리오 3: Lock 대기 상황 분석
    ------------------------------
    락 대기 중인 트랜잭션을 찾고 분석합니다.
    """
    print_section("시나리오 3: Lock 대기 상황 분석")

    conn_t1 = get_connection()
    conn_t2 = get_connection()
    conn_monitor = get_connection(autocommit=True)

    cur_t1 = conn_t1.cursor()
    cur_t2 = conn_t2.cursor()
    cur_monitor = conn_monitor.cursor()

    wait_event = threading.Event()

    def t1_hold_lock():
        cur_t1.execute("BEGIN")
        cur_t1.execute("UPDATE accounts SET balance = balance WHERE name = 'Alice'")
        print("[T1] Alice row 락 획득, 5초간 유지...")
        wait_event.set()  # T2 시작 신호
        time.sleep(5)
        conn_t1.commit()
        print("[T1] COMMIT 완료")

    def t2_wait_lock():
        wait_event.wait()  # T1이 락 획득할 때까지 대기
        time.sleep(0.5)
        cur_t2.execute("BEGIN")
        print("[T2] Alice row UPDATE 시도 (락 대기 중...)")
        cur_t2.execute("UPDATE accounts SET balance = balance WHERE name = 'Alice'")
        print("[T2] 락 획득! UPDATE 완료")
        conn_t2.commit()

    print("""
    시나리오:
    T1: Alice row 락 획득 후 5초간 유지
    T2: 같은 row UPDATE 시도 → 대기
    Monitor: 대기 상황 관찰
    """)

    t1 = threading.Thread(target=t1_hold_lock)
    t2 = threading.Thread(target=t2_wait_lock)

    t1.start()
    t2.start()

    # 대기 상황이 발생할 때까지 잠시 대기
    time.sleep(1)

    # 락 대기 상황 확인
    print("\n[Monitor] 락 대기 상황 조회")
    cur_monitor.execute("""
        SELECT * FROM v_lock_waits
    """)
    print_result(cur_monitor, "v_lock_waits 뷰 결과")

    # pg_stat_activity에서 waiting 상태 확인
    cur_monitor.execute("""
        SELECT
            pid,
            usename,
            state,
            wait_event_type,
            wait_event,
            LEFT(query, 50) as query
        FROM pg_stat_activity
        WHERE state = 'active'
        AND pid != pg_backend_pid()
    """)
    print_result(cur_monitor, "활성 세션 상태")

    t1.join()
    t2.join()

    print("""
    락 대기 확인 방법:
    1. v_lock_waits 뷰 (init.sql에서 생성)
    2. pg_stat_activity의 wait_event
    3. pg_locks의 granted = false

    문제 해결:
    - 대기가 길면 blocking 세션 확인
    - 필요시 pg_terminate_backend()로 강제 종료
        """)

    cur_t1.close()
    cur_t2.close()
    cur_monitor.close()
    conn_t1.close()
    conn_t2.close()
    conn_monitor.close()


def scenario_4_deadlock():
    """
    시나리오 4: Deadlock 발생시키기
    ------------------------------
    의도적으로 deadlock을 발생시키고 PostgreSQL의 처리를 관찰합니다.
    """
    print_section("시나리오 4: Deadlock 발생시키기")

    conn_t1 = get_connection()
    conn_t2 = get_connection()

    cur_t1 = conn_t1.cursor()
    cur_t2 = conn_t2.cursor()

    results = {'t1': None, 't2': None}
    barrier = threading.Barrier(2)

    def transaction_1():
        try:
            cur_t1.execute("BEGIN")
            print("[T1] BEGIN")

            # Alice 락 획득
            print("[T1] UPDATE Alice")
            cur_t1.execute("UPDATE accounts SET balance = balance WHERE name = 'Alice'")
            print("[T1] Alice 락 획득!")

            barrier.wait()  # T2와 동기화
            time.sleep(0.5)

            # Bob 락 시도 → 대기
            print("[T1] UPDATE Bob 시도...")
            cur_t1.execute("UPDATE accounts SET balance = balance WHERE name = 'Bob'")
            conn_t1.commit()
            print("[T1] 성공!")
            results['t1'] = 'success'

        except errors.DeadlockDetected as e:
            print("[T1] DEADLOCK 감지됨!")
            results['t1'] = 'deadlock'
            conn_t1.rollback()
        except Exception as e:
            print(f"[T1] 오류: {type(e).__name__}")
            results['t1'] = 'error'
            conn_t1.rollback()

    def transaction_2():
        try:
            cur_t2.execute("BEGIN")
            print("[T2] BEGIN")

            # Bob 락 획득
            print("[T2] UPDATE Bob")
            cur_t2.execute("UPDATE accounts SET balance = balance WHERE name = 'Bob'")
            print("[T2] Bob 락 획득!")

            barrier.wait()  # T1과 동기화
            time.sleep(0.5)

            # Alice 락 시도 → 대기 (여기서 deadlock!)
            print("[T2] UPDATE Alice 시도...")
            cur_t2.execute("UPDATE accounts SET balance = balance WHERE name = 'Alice'")
            conn_t2.commit()
            print("[T2] 성공!")
            results['t2'] = 'success'

        except errors.DeadlockDetected as e:
            print("[T2] DEADLOCK 감지됨!")
            results['t2'] = 'deadlock'
            conn_t2.rollback()
        except Exception as e:
            print(f"[T2] 오류: {type(e).__name__}")
            results['t2'] = 'error'
            conn_t2.rollback()

    print("""
    Deadlock 시나리오:
    T1: Alice 락 획득 → Bob 락 시도
    T2: Bob 락 획득 → Alice 락 시도
    → 서로가 서로를 기다리는 교착 상태!
    """)

    t1 = threading.Thread(target=transaction_1)
    t2 = threading.Thread(target=transaction_2)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    print(f"""
    결과:
    T1: {results['t1']}
    T2: {results['t2']}

    PostgreSQL의 Deadlock 처리:
    1. deadlock_timeout (기본 1초) 후 감지 시작
    2. Wait-for 그래프에서 사이클 탐지
    3. 하나의 트랜잭션을 선택하여 abort
    4. 나머지 트랜잭션은 계속 진행

    Deadlock 방지 전략:
    1. 항상 같은 순서로 락 획득 (Alice → Bob)
    2. 트랜잭션을 짧게 유지
    3. SELECT FOR UPDATE NOWAIT 사용
        """)

    cur_t1.close()
    cur_t2.close()
    conn_t1.close()
    conn_t2.close()


def scenario_5_advisory_locks():
    """
    시나리오 5: Advisory Lock
    -------------------------
    애플리케이션 레벨의 락을 사용합니다.
    """
    print_section("시나리오 5: Advisory Lock")

    conn_t1 = get_connection()
    conn_t2 = get_connection()

    cur_t1 = conn_t1.cursor()
    cur_t2 = conn_t2.cursor()

    try:
        print("""
    Advisory Lock이란?
    - PostgreSQL이 제공하는 애플리케이션 레벨 락
    - 테이블/row와 무관하게 임의의 리소스에 락
    - 숫자 ID로 락 식별

    사용 예:
    - 배치 작업 중복 실행 방지
    - 분산 환경에서 리더 선출
    - 특정 리소스 접근 제어
        """)

        LOCK_ID = 12345  # 임의의 락 ID

        # T1: Advisory 락 획득 (session level)
        print(f"\n[T1] pg_advisory_lock({LOCK_ID}) 획득 시도")
        cur_t1.execute("SELECT pg_advisory_lock(%s)", (LOCK_ID,))
        print("[T1] Advisory 락 획득 성공!")

        # T2: 같은 락 시도 (non-blocking)
        print(f"\n[T2] pg_try_advisory_lock({LOCK_ID}) 시도")
        cur_t2.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_ID,))
        result = cur_t2.fetchone()[0]
        print(f"[T2] 결과: {result} ({'성공' if result else '실패 - 이미 사용 중'})")

        # T1: 락 해제
        print(f"\n[T1] pg_advisory_unlock({LOCK_ID}) 해제")
        cur_t1.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))

        # T2: 다시 시도
        print(f"\n[T2] 다시 pg_try_advisory_lock({LOCK_ID}) 시도")
        cur_t2.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_ID,))
        result = cur_t2.fetchone()[0]
        print(f"[T2] 결과: {result} ({'성공' if result else '실패'})")

        # 정리
        cur_t2.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))

        print("""
    Advisory Lock 함수:
    - pg_advisory_lock(id): 획득 (대기)
    - pg_try_advisory_lock(id): 획득 시도 (즉시 반환)
    - pg_advisory_unlock(id): 해제
    - pg_advisory_xact_lock(id): 트랜잭션 종료 시 자동 해제
        """)

    finally:
        cur_t1.close()
        cur_t2.close()
        conn_t1.close()
        conn_t2.close()


def main():
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║              Lab 06: Lock 모니터링 실습                     ║
    ║                                                           ║
    ║  PostgreSQL의 락 메커니즘과 모니터링 방법을 학습합니다.       ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    try:
        scenario_1_lock_types()
        scenario_2_row_level_lock()
        scenario_3_lock_waiting()
        scenario_4_deadlock()
        scenario_5_advisory_locks()

        print_section("Lab 06 완료!")
        print("""
    학습 정리:

    1. Lock 유형 (약한 순):
       ACCESS SHARE → ROW SHARE → ROW EXCLUSIVE →
       SHARE UPDATE EXCLUSIVE → SHARE → EXCLUSIVE →
       ACCESS EXCLUSIVE

    2. 모니터링:
       - pg_locks: 현재 락 상태
       - pg_stat_activity: 세션 상태, wait_event
       - v_lock_waits: 락 대기 관계 (커스텀 뷰)

    3. Deadlock:
       - PostgreSQL이 자동 감지 및 해결
       - 하나의 트랜잭션 abort
       - 애플리케이션에서 재시도 로직 필요

    4. Advisory Lock:
       - 애플리케이션 레벨 동시성 제어
       - 배치 작업 중복 방지 등에 활용

    전체 실습 완료! README.md를 참고하여 복습하세요.
        """)

    except psycopg2.OperationalError as e:
        print(f"\n오류: 데이터베이스에 연결할 수 없습니다.")
        print(f"Docker가 실행 중인지 확인하세요: docker-compose up -d")
        print(f"상세 오류: {e}")


if __name__ == "__main__":
    main()
