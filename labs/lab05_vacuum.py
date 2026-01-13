"""
Lab 05: VACUUM과 Dead Tuple 실습
================================

학습 목표:
- Dead tuple이 무엇인지 이해
- VACUUM의 역할과 동작 방식
- pg_stat_user_tables로 테이블 상태 모니터링
- autovacuum 설정 확인

실행 방법:
    python lab05_vacuum.py
"""

import psycopg2
from tabulate import tabulate
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


def scenario_1_create_dead_tuples():
    """
    시나리오 1: Dead Tuple 생성하기
    ------------------------------
    UPDATE/DELETE로 dead tuple을 생성하고 확인합니다.
    """
    print_section("시나리오 1: Dead Tuple 생성하기")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    try:
        # 테스트 테이블 초기화
        print("\n[테이블 초기화]")
        cur.execute("TRUNCATE vacuum_test RESTART IDENTITY")
        cur.execute("""
            INSERT INTO vacuum_test (data)
            SELECT 'row_' || generate_series(1, 1000)
        """)
        print("1000개 row INSERT 완료")

        # 통계 갱신
        cur.execute("ANALYZE vacuum_test")

        # 초기 상태 확인
        cur.execute("""
            SELECT
                relname as table_name,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                n_tup_ins as total_inserts,
                n_tup_upd as total_updates,
                n_tup_del as total_deletes
            FROM pg_stat_user_tables
            WHERE relname = 'vacuum_test'
        """)
        print_result(cur, "초기 상태 (INSERT 직후)")

        # 모든 row UPDATE → 1000개 dead tuple 생성
        print("\n[모든 row UPDATE]")
        cur.execute("""
            UPDATE vacuum_test
            SET data = data || '_updated',
                updated_at = CURRENT_TIMESTAMP
        """)
        print("1000개 row UPDATE 완료 (1000개 dead tuple 생성됨)")

        # 통계 갱신 대기 (약간의 딜레이)
        time.sleep(0.5)
        cur.execute("ANALYZE vacuum_test")

        # UPDATE 후 상태 확인
        cur.execute("""
            SELECT
                relname as table_name,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                n_tup_upd as total_updates
            FROM pg_stat_user_tables
            WHERE relname = 'vacuum_test'
        """)
        print_result(cur, "UPDATE 후 상태")

        # pageinspect로 raw 확인 (첫 페이지만)
        cur.execute("""
            SELECT COUNT(*) as tuple_count
            FROM heap_page_items(get_raw_page('vacuum_test', 0))
            WHERE t_data IS NOT NULL
        """)
        result = cur.fetchone()
        print(f"\n페이지 0의 실제 튜플 수: {result[0]}개")
        print("(live tuple + dead tuple이 모두 물리적으로 존재)")

        print("""
    분석:
    - n_live_tup: 현재 유효한(visible) 튜플 수
    - n_dead_tup: 삭제/수정되어 더 이상 필요 없는 튜플 수
    - UPDATE는 기존 튜플을 dead로 표시하고 새 튜플 생성
    - Dead tuple은 공간을 차지하지만 쿼리에서 보이지 않음
        """)

    finally:
        cur.close()
        conn.close()


def scenario_2_vacuum_effect():
    """
    시나리오 2: VACUUM 실행 효과
    ---------------------------
    VACUUM 전후의 상태 변화를 확인합니다.
    """
    print_section("시나리오 2: VACUUM 실행 효과")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    try:
        # VACUUM 전 상태
        cur.execute("""
            SELECT
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                last_vacuum,
                last_autovacuum
            FROM pg_stat_user_tables
            WHERE relname = 'vacuum_test'
        """)
        print_result(cur, "VACUUM 전 상태")

        # VACUUM 실행
        print("\n[VACUUM VERBOSE 실행]")
        print("-" * 40)

        # VACUUM VERBOSE의 출력을 보려면 서버 로그 확인 필요
        # 여기서는 간단히 실행
        cur.execute("VACUUM vacuum_test")
        print("VACUUM 완료!")

        # 통계 갱신
        time.sleep(0.5)

        # VACUUM 후 상태
        cur.execute("""
            SELECT
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                last_vacuum,
                last_autovacuum
            FROM pg_stat_user_tables
            WHERE relname = 'vacuum_test'
        """)
        print_result(cur, "VACUUM 후 상태")

        print("""
    VACUUM의 역할:
    1. Dead tuple을 정리하여 공간 재사용 가능하게 함
    2. Visibility Map 갱신
    3. 트랜잭션 ID wraparound 방지

    주의: VACUUM은 공간을 OS에 반환하지 않음!
    → VACUUM FULL이 필요하지만, 테이블 전체 락 발생
        """)

    finally:
        cur.close()
        conn.close()


def scenario_3_table_bloat():
    """
    시나리오 3: Table Bloat 확인
    ---------------------------
    테이블 크기와 실제 데이터 비율을 확인합니다.
    """
    print_section("시나리오 3: Table Bloat 확인")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    try:
        # 테이블 크기 정보
        cur.execute("""
            SELECT
                pg_size_pretty(pg_table_size('vacuum_test')) as table_size,
                pg_size_pretty(pg_indexes_size('vacuum_test')) as index_size,
                pg_size_pretty(pg_total_relation_size('vacuum_test')) as total_size
        """)
        print_result(cur, "테이블 크기")

        # Dead tuple ratio 계산
        cur.execute("""
            SELECT
                n_live_tup,
                n_dead_tup,
                CASE
                    WHEN n_live_tup + n_dead_tup = 0 THEN 0
                    ELSE ROUND(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
                END as dead_tuple_ratio_pct
            FROM pg_stat_user_tables
            WHERE relname = 'vacuum_test'
        """)
        print_result(cur, "Dead Tuple 비율")

        # 다시 UPDATE해서 bloat 생성
        print("\n[Bloat 생성: 3회 연속 UPDATE]")
        for i in range(3):
            cur.execute("""
                UPDATE vacuum_test
                SET data = data || '_v' || %s,
                    updated_at = CURRENT_TIMESTAMP
            """, (i+2,))
            print(f"  UPDATE {i+1}/3 완료")

        time.sleep(0.5)
        cur.execute("ANALYZE vacuum_test")

        # Bloat 후 상태
        cur.execute("""
            SELECT
                pg_size_pretty(pg_table_size('vacuum_test')) as table_size,
                n_live_tup,
                n_dead_tup,
                CASE
                    WHEN n_live_tup + n_dead_tup = 0 THEN 0
                    ELSE ROUND(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
                END as dead_ratio_pct
            FROM pg_stat_user_tables
            WHERE relname = 'vacuum_test'
        """)
        print_result(cur, "연속 UPDATE 후 상태")

        print("""
    Table Bloat란?
    - Dead tuple로 인해 테이블이 실제 데이터보다 커진 상태
    - 쿼리 성능 저하 (더 많은 페이지 스캔)
    - 디스크 공간 낭비

    해결책:
    1. 정기적인 VACUUM (dead tuple 재사용)
    2. VACUUM FULL (테이블 재작성, 락 주의!)
    3. pg_repack (온라인 테이블 재구성)
        """)

    finally:
        cur.close()
        conn.close()


def scenario_4_vacuum_full():
    """
    시나리오 4: VACUUM FULL vs VACUUM
    ---------------------------------
    VACUUM FULL로 실제 공간 회수하기
    """
    print_section("시나리오 4: VACUUM FULL vs VACUUM")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    try:
        # 현재 상태
        cur.execute("""
            SELECT pg_size_pretty(pg_table_size('vacuum_test')) as table_size
        """)
        before_size = cur.fetchone()[0]
        print(f"현재 테이블 크기: {before_size}")

        # 일반 VACUUM
        print("\n[일반 VACUUM 실행]")
        cur.execute("VACUUM vacuum_test")

        cur.execute("""
            SELECT pg_size_pretty(pg_table_size('vacuum_test')) as table_size
        """)
        after_vacuum = cur.fetchone()[0]
        print(f"VACUUM 후 크기: {after_vacuum}")

        # VACUUM FULL
        print("\n[VACUUM FULL 실행]")
        print("주의: VACUUM FULL은 테이블에 ACCESS EXCLUSIVE 락을 겁니다!")
        cur.execute("VACUUM FULL vacuum_test")

        cur.execute("""
            SELECT pg_size_pretty(pg_table_size('vacuum_test')) as table_size
        """)
        after_full = cur.fetchone()[0]
        print(f"VACUUM FULL 후 크기: {after_full}")

        print(f"""
    비교:
    - 초기 크기: {before_size}
    - VACUUM 후: {after_vacuum} (공간 재사용 가능, 크기 유지)
    - VACUUM FULL 후: {after_full} (실제 공간 회수)

    VACUUM vs VACUUM FULL:
    ┌─────────────────┬──────────────────┬─────────────────────┐
    │                 │ VACUUM           │ VACUUM FULL         │
    ├─────────────────┼──────────────────┼─────────────────────┤
    │ 락              │ ShareUpdateExcl  │ AccessExclusive     │
    │ 읽기/쓰기 가능  │ 예               │ 아니오              │
    │ 공간 회수       │ 재사용만         │ OS에 반환           │
    │ 속도            │ 빠름             │ 느림 (재작성)       │
    │ 사용 시점       │ 정기적           │ 큰 bloat 해결 시    │
    └─────────────────┴──────────────────┴─────────────────────┘
        """)

    finally:
        cur.close()
        conn.close()


def scenario_5_autovacuum_settings():
    """
    시나리오 5: Autovacuum 설정 확인
    -------------------------------
    자동 VACUUM 설정을 확인하고 이해합니다.
    """
    print_section("시나리오 5: Autovacuum 설정")

    conn = get_connection(autocommit=True)
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT name, setting, short_desc
            FROM pg_settings
            WHERE name LIKE 'autovacuum%'
            ORDER BY name
        """)
        print_result(cur, "Autovacuum 관련 설정")

        print("""
    주요 설정 설명:

    autovacuum = on
      자동 VACUUM 활성화 여부

    autovacuum_vacuum_threshold = 50
      VACUUM 트리거 기본 임계값 (dead tuple 수)

    autovacuum_vacuum_scale_factor = 0.2
      테이블 크기 대비 비율 (20%)

    VACUUM 트리거 조건:
      dead_tuples > threshold + scale_factor * n_live_tup
      예: 1000 row 테이블 → 50 + 0.2 * 1000 = 250개 dead tuple

    autovacuum_naptime = 1min
      autovacuum 데몬 실행 간격

    테이블별 설정 오버라이드:
      ALTER TABLE accounts SET (autovacuum_vacuum_threshold = 100);
        """)

        # 테이블별 설정 확인
        cur.execute("""
            SELECT
                c.relname as table_name,
                c.reloptions as table_options
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public'
            AND c.relkind = 'r'
            AND c.reloptions IS NOT NULL
        """)
        result = cur.fetchall()
        if result:
            print("\n테이블별 커스텀 설정:")
            for row in result:
                print(f"  {row[0]}: {row[1]}")
        else:
            print("\n(테이블별 커스텀 autovacuum 설정 없음)")

    finally:
        cur.close()
        conn.close()


def main():
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║           Lab 05: VACUUM과 Dead Tuple 실습                 ║
    ║                                                           ║
    ║  PostgreSQL이 삭제된 데이터를 어떻게 정리하는지 학습합니다.    ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    try:
        scenario_1_create_dead_tuples()
        scenario_2_vacuum_effect()
        scenario_3_table_bloat()
        scenario_4_vacuum_full()
        scenario_5_autovacuum_settings()

        print_section("Lab 05 완료!")
        print("""
    학습 정리:

    1. Dead Tuple:
       - UPDATE/DELETE된 튜플의 이전 버전
       - 물리적으로 존재하지만 쿼리에서 invisible
       - VACUUM이 정리

    2. VACUUM 종류:
       - VACUUM: dead tuple 공간을 재사용 가능하게 표시
       - VACUUM FULL: 테이블 재작성, 실제 공간 회수

    3. Table Bloat:
       - 과도한 dead tuple로 테이블 비대화
       - 쿼리 성능 저하, 디스크 낭비
       - 정기 VACUUM으로 방지

    4. Autovacuum:
       - PostgreSQL이 자동으로 VACUUM 실행
       - threshold + scale_factor 기반 트리거
       - 테이블별 설정 커스터마이징 가능

    다음 실습: lab06_locks.py
        """)

    except psycopg2.OperationalError as e:
        print(f"\n오류: 데이터베이스에 연결할 수 없습니다.")
        print(f"Docker가 실행 중인지 확인하세요: docker-compose up -d")
        print(f"상세 오류: {e}")


if __name__ == "__main__":
    main()
