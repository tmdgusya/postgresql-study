#!/usr/bin/env python3
"""
Lab 10: 성능 모니터링과 튜닝
==========================

학습 목표:
- pg_stat_statements로 쿼리 성능 분석
- pg_stat_user_indexes로 인덱스 활용도 분석
- pgstattuple로 인덱스/테이블 bloat 감지
- matplotlib로 성능 지표 시각화

선수 지식: Lab 07~09

필요 확장:
- pg_stat_statements (쿼리 통계)
- pgstattuple (물리적 통계)
"""

import psycopg2
from psycopg2 import sql
from tabulate import tabulate
import time
import os

# matplotlib 설정
import matplotlib
matplotlib.use('Agg')  # GUI 없이 파일로 저장
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np

# 한글 폰트 설정 (시스템에 따라 조정 필요)
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False

# 데이터베이스 연결 설정
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'mvcc_lab',
    'user': 'study',
    'password': 'study123'
}

# 그래프 저장 디렉토리
GRAPH_DIR = os.path.join(os.path.dirname(__file__), 'graphs')
os.makedirs(GRAPH_DIR, exist_ok=True)


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


def save_graph(fig, filename):
    """그래프를 파일로 저장"""
    filepath = os.path.join(GRAPH_DIR, filename)
    fig.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f"\n[Graph Saved] {filepath}")
    return filepath


# =============================================================================
# 시나리오 1: pg_stat_statements - 쿼리 성능 분석
# =============================================================================

def scenario_1_query_stats():
    """
    시나리오 1: pg_stat_statements로 쿼리 성능 분석

    가장 느린 쿼리, 가장 많이 호출되는 쿼리를 식별합니다.
    """
    print_section("시나리오 1: pg_stat_statements - 쿼리 성능 분석")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ pg_stat_statements 개요                                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 모든 SQL 쿼리의 실행 통계를 수집하는 확장                         │
│                                                                  │
│ 주요 지표:                                                       │
│   calls          : 호출 횟수                                     │
│   total_exec_time: 총 실행 시간 (ms)                             │
│   mean_exec_time : 평균 실행 시간                                │
│   rows           : 반환/영향 받은 행 수                          │
│   shared_blks_hit: 버퍼 캐시 히트                                │
│   shared_blks_read: 디스크 읽기                                  │
│                                                                  │
│ ★ 활용:                                                          │
│   - 느린 쿼리 Top N 식별                                         │
│   - 자주 호출되는 쿼리 식별                                      │
│   - 캐시 히트율 분석                                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 샘플 쿼리 실행하여 통계 생성
        print_subsection("1-0: 샘플 쿼리 실행 (통계 생성)")

        sample_queries = [
            "SELECT COUNT(*) FROM orders WHERE customer_id = 100",
            "SELECT AVG(total_amount) FROM orders WHERE status = 'pending'",
            "SELECT * FROM orders WHERE order_date > CURRENT_DATE - 7 LIMIT 10",
            "SELECT customer_id, SUM(total_amount) FROM orders GROUP BY customer_id LIMIT 50",
            "SELECT * FROM products_json WHERE attributes @> '{\"brand\": \"TechCo\"}'",
        ]

        for q in sample_queries:
            for _ in range(5):  # 각 쿼리 5회 실행
                cur.execute(q)
                cur.fetchall()

        print("샘플 쿼리 실행 완료 (통계 수집됨)")

        # 1-1: Top 10 느린 쿼리
        print_subsection("1-1: 총 실행 시간 Top 10 쿼리")

        rows = execute_and_show(cur, """
            SELECT
                LEFT(query, 50) as query_preview,
                calls,
                ROUND(total_exec_time::numeric, 2) as total_time_ms,
                ROUND(mean_exec_time::numeric, 2) as mean_time_ms,
                rows
            FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND query NOT LIKE '%pg_stat%'
            ORDER BY total_exec_time DESC
            LIMIT 10
        """, "총 실행 시간 기준")

        # 1-2: 호출 빈도 Top 10
        print_subsection("1-2: 호출 빈도 Top 10 쿼리")

        execute_and_show(cur, """
            SELECT
                LEFT(query, 50) as query_preview,
                calls,
                ROUND(total_exec_time::numeric, 2) as total_time_ms,
                ROUND((total_exec_time / NULLIF(calls, 0))::numeric, 2) as avg_time_ms
            FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND query NOT LIKE '%pg_stat%'
            ORDER BY calls DESC
            LIMIT 10
        """, "호출 빈도 기준")

        # 1-3: 버퍼 캐시 히트율 분석
        print_subsection("1-3: 버퍼 캐시 히트율")

        cache_data = execute_and_show(cur, """
            SELECT
                LEFT(query, 40) as query_preview,
                calls,
                shared_blks_hit as cache_hits,
                shared_blks_read as disk_reads,
                CASE
                    WHEN shared_blks_hit + shared_blks_read = 0 THEN 100
                    ELSE ROUND(
                        100.0 * shared_blks_hit /
                        (shared_blks_hit + shared_blks_read), 1
                    )
                END as hit_rate_pct
            FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND (shared_blks_hit + shared_blks_read) > 0
            AND query NOT LIKE '%pg_stat%'
            ORDER BY (shared_blks_hit + shared_blks_read) DESC
            LIMIT 10
        """, "캐시 히트율 (높을수록 좋음)")

        # 1-4: 시각화 - 쿼리 실행 시간 분포
        print_subsection("1-4: 쿼리 실행 시간 분포 (Graph)")

        cur.execute("""
            SELECT
                CASE
                    WHEN mean_exec_time < 1 THEN '< 1ms'
                    WHEN mean_exec_time < 10 THEN '1-10ms'
                    WHEN mean_exec_time < 100 THEN '10-100ms'
                    WHEN mean_exec_time < 1000 THEN '100ms-1s'
                    ELSE '> 1s'
                END as time_bucket,
                COUNT(*) as query_count
            FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            GROUP BY 1
            ORDER BY
                CASE
                    WHEN mean_exec_time < 1 THEN 1
                    WHEN mean_exec_time < 10 THEN 2
                    WHEN mean_exec_time < 100 THEN 3
                    WHEN mean_exec_time < 1000 THEN 4
                    ELSE 5
                END
        """)
        time_dist = cur.fetchall()

        if time_dist:
            fig, ax = plt.subplots(figsize=(10, 6))
            buckets = [r[0] for r in time_dist]
            counts = [r[1] for r in time_dist]

            colors = ['#2ecc71', '#3498db', '#f1c40f', '#e67e22', '#e74c3c'][:len(buckets)]
            bars = ax.bar(buckets, counts, color=colors, edgecolor='black')

            ax.set_xlabel('Execution Time Bucket', fontsize=12)
            ax.set_ylabel('Number of Query Types', fontsize=12)
            ax.set_title('Query Execution Time Distribution', fontsize=14, fontweight='bold')

            # 값 표시
            for bar, count in zip(bars, counts):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                       str(count), ha='center', va='bottom', fontweight='bold')

            ax.set_ylim(0, max(counts) * 1.2)
            plt.tight_layout()
            save_graph(fig, 'query_time_distribution.png')

        print("""
★ 핵심 정리:
  1. pg_stat_statements는 쿼리 성능 분석의 핵심 도구
  2. total_exec_time으로 가장 비용이 큰 쿼리 식별
  3. calls로 자주 호출되는 쿼리 식별 (캐싱 후보)
  4. cache hit rate 90% 이상이 이상적
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 2: pg_stat_user_indexes - 인덱스 활용도 분석
# =============================================================================

def scenario_2_index_usage():
    """
    시나리오 2: pg_stat_user_indexes로 인덱스 활용도 분석

    미사용 인덱스를 찾아 삭제 후보를 식별합니다.
    """
    print_section("시나리오 2: 인덱스 활용도 분석")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ 인덱스 활용도 모니터링의 중요성                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 인덱스 비용:                                                     │
│   - 저장 공간 차지                                               │
│   - INSERT/UPDATE 시 인덱스도 갱신 → 쓰기 성능 저하              │
│   - VACUUM 대상 증가                                             │
│                                                                  │
│ 미사용 인덱스:                                                   │
│   - 비용만 발생, 이점 없음                                       │
│   - 정기적으로 확인하여 삭제 필요                                 │
│                                                                  │
│ ★ 주의: idx_scan=0이라도 바로 삭제 금지!                         │
│   - 월별 배치 쿼리에서만 사용될 수 있음                          │
│   - 재해 복구용일 수 있음                                        │
│   - 충분한 관찰 기간 필요 (최소 1-2주)                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 2-1: 전체 인덱스 현황
        print_subsection("2-1: 전체 인덱스 현황")

        index_data = execute_and_show(cur, """
            SELECT
                relname as table_name,
                indexrelname as index_name,
                pg_size_pretty(pg_relation_size(indexrelid)) as size,
                idx_scan as scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched
            FROM pg_stat_user_indexes
            WHERE schemaname = 'public'
            ORDER BY idx_scan DESC, pg_relation_size(indexrelid) DESC
        """, "모든 인덱스 활용 현황")

        # 2-2: 미사용/저사용 인덱스
        print_subsection("2-2: 미사용/저사용 인덱스 (삭제 후보)")

        unused = execute_and_show(cur, """
            SELECT
                relname as table_name,
                indexrelname as index_name,
                pg_size_pretty(pg_relation_size(indexrelid)) as size,
                idx_scan as scans,
                CASE
                    WHEN idx_scan = 0 THEN 'UNUSED'
                    WHEN idx_scan < 10 THEN 'LOW USAGE'
                    ELSE 'USED'
                END as status
            FROM pg_stat_user_indexes
            WHERE schemaname = 'public'
            AND indexrelname NOT LIKE '%pkey%'  -- PK 제외
            AND idx_scan < 10
            ORDER BY pg_relation_size(indexrelid) DESC
        """, "미사용/저사용 인덱스")

        # 2-3: 인덱스 효율성 분석
        print_subsection("2-3: 인덱스 효율성 분석")

        execute_and_show(cur, """
            SELECT
                relname as table_name,
                indexrelname as index_name,
                idx_scan as scans,
                idx_tup_read as read,
                idx_tup_fetch as fetched,
                CASE
                    WHEN idx_tup_read = 0 THEN 0
                    ELSE ROUND(100.0 * idx_tup_fetch / idx_tup_read, 1)
                END as fetch_efficiency_pct
            FROM pg_stat_user_indexes
            WHERE schemaname = 'public'
            AND idx_scan > 0
            ORDER BY idx_scan DESC
            LIMIT 10
        """, "인덱스 효율성 (fetch/read 비율)")

        print("""
★ fetch_efficiency_pct 해석:
  - 100% 근처: 읽은 인덱스 항목 대부분 실제 사용 (효율적)
  - 낮은 값: 인덱스로 많이 읽지만 실제 사용은 적음 (비효율)
    → 인덱스 조건 검토 필요
        """)

        # 2-4: 시각화 - 인덱스 사용률/크기 버블 차트
        print_subsection("2-4: 인덱스 크기 vs 사용률 (Graph)")

        cur.execute("""
            SELECT
                indexrelname,
                pg_relation_size(indexrelid) as size_bytes,
                idx_scan
            FROM pg_stat_user_indexes
            WHERE schemaname = 'public'
            AND pg_relation_size(indexrelid) > 0
            LIMIT 20
        """)
        idx_stats = cur.fetchall()

        if idx_stats:
            fig, ax = plt.subplots(figsize=(12, 8))

            names = [r[0][:20] for r in idx_stats]
            sizes = [r[1] / 1024 for r in idx_stats]  # KB
            scans = [r[2] for r in idx_stats]

            # 색상: 스캔 횟수 기반
            colors = ['#e74c3c' if s == 0 else '#f1c40f' if s < 10 else '#2ecc71'
                     for s in scans]

            bars = ax.barh(names, sizes, color=colors, edgecolor='black')

            ax.set_xlabel('Index Size (KB)', fontsize=12)
            ax.set_ylabel('Index Name', fontsize=12)
            ax.set_title('Index Size and Usage Status\n(Red=Unused, Yellow=Low, Green=Active)',
                        fontsize=14, fontweight='bold')

            # 스캔 횟수 표시
            for bar, scan in zip(bars, scans):
                ax.text(bar.get_width() + 5, bar.get_y() + bar.get_height()/2,
                       f'{scan} scans', va='center', fontsize=9)

            plt.tight_layout()
            save_graph(fig, 'index_usage_chart.png')

        print("""
★ 핵심 정리:
  1. pg_stat_user_indexes로 인덱스 사용량 모니터링
  2. idx_scan=0인 인덱스는 삭제 후보 (충분한 관찰 후)
  3. 크기가 큰 미사용 인덱스 우선 검토
  4. PK/FK 인덱스는 특별한 경우 아니면 유지
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 3: Index Bloat 감지
# =============================================================================

def scenario_3_index_bloat():
    """
    시나리오 3: pgstattuple로 인덱스 Bloat 감지

    인덱스 내 빈 공간(bloat)을 측정하고 REINDEX 필요성을 판단합니다.
    """
    print_section("시나리오 3: Index Bloat 감지")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ Index Bloat이란?                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ MVCC로 인해 발생하는 인덱스 내 빈 공간                            │
│                                                                  │
│ 원인:                                                            │
│   1. DELETE: 인덱스 항목이 바로 제거되지 않음                     │
│   2. UPDATE: 인덱스 컬럼 변경 시 새 항목 추가, 이전 항목 남음     │
│   3. VACUUM: dead tuple만 정리, 빈 공간은 남음                   │
│                                                                  │
│ 문제:                                                            │
│   - 인덱스 크기 증가 → 메모리 사용량 증가                        │
│   - 스캔 시 불필요한 페이지 읽기 → 성능 저하                     │
│                                                                  │
│ 해결:                                                            │
│   - REINDEX: 인덱스 재구축                                       │
│   - REINDEX CONCURRENTLY: 락 없이 재구축 (PostgreSQL 12+)        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        # 3-1: Bloat 유발 (시뮬레이션)
        print_subsection("3-1: Bloat 생성 (UPDATE 반복)")

        # 테스트 테이블에 데이터 추가
        cur.execute("DELETE FROM index_mvcc_test WHERE id > 1000")
        cur.execute("""
            INSERT INTO index_mvcc_test (indexed_col, non_indexed_col, data)
            SELECT i, i * 10, 'bloat_test_' || i
            FROM generate_series(1001, 2000) i
            ON CONFLICT DO NOTHING
        """)
        conn.commit()

        # 인덱스 컬럼 반복 UPDATE
        print("인덱스 컬럼 반복 UPDATE (bloat 유발)...")
        for i in range(3):
            cur.execute("""
                UPDATE index_mvcc_test
                SET indexed_col = indexed_col + 1
                WHERE id BETWEEN 1001 AND 1500
            """)
            conn.commit()

        print("UPDATE 완료. VACUUM 전 상태 확인...")

        # 3-2: pgstattuple로 bloat 측정
        print_subsection("3-2: pgstattuple로 Bloat 측정")

        try:
            bloat_data = execute_and_show(cur, """
                SELECT
                    'idx_mvcc_indexed' as index_name,
                    pg_size_pretty(index_size) as total_size,
                    avg_leaf_density as leaf_density_pct,
                    leaf_fragmentation as fragmentation_pct
                FROM pgstatindex('idx_mvcc_indexed')
            """, "인덱스 통계 (VACUUM 전)")
        except Exception as e:
            print(f"pgstatindex 실행 불가: {e}")
            bloat_data = None

        # 3-3: VACUUM 실행
        print_subsection("3-3: VACUUM 후 비교")

        cur.execute("VACUUM ANALYZE index_mvcc_test")
        conn.commit()

        try:
            execute_and_show(cur, """
                SELECT
                    'idx_mvcc_indexed' as index_name,
                    pg_size_pretty(index_size) as total_size,
                    avg_leaf_density as leaf_density_pct,
                    leaf_fragmentation as fragmentation_pct
                FROM pgstatindex('idx_mvcc_indexed')
            """, "인덱스 통계 (VACUUM 후)")
        except:
            pass

        print("""
★ 지표 해석:
  - leaf_density_pct: 리프 페이지 공간 활용률 (높을수록 좋음, 90%+ 이상적)
  - fragmentation_pct: 단편화 비율 (낮을수록 좋음)

  Bloat 판단 기준:
  - leaf_density < 70%: REINDEX 권장
  - fragmentation > 20%: REINDEX 고려
        """)

        # 3-4: REINDEX 실행
        print_subsection("3-4: REINDEX 실행")

        print("\nREINDEX 전 크기:")
        cur.execute("""
            SELECT pg_size_pretty(pg_relation_size('idx_mvcc_indexed'))
        """)
        print(f"  인덱스 크기: {cur.fetchone()[0]}")

        # REINDEX 실행
        cur.execute("REINDEX INDEX idx_mvcc_indexed")
        conn.commit()

        print("\nREINDEX 후 크기:")
        cur.execute("""
            SELECT pg_size_pretty(pg_relation_size('idx_mvcc_indexed'))
        """)
        print(f"  인덱스 크기: {cur.fetchone()[0]}")

        try:
            execute_and_show(cur, """
                SELECT
                    'idx_mvcc_indexed' as index_name,
                    pg_size_pretty(index_size) as total_size,
                    avg_leaf_density as leaf_density_pct,
                    leaf_fragmentation as fragmentation_pct
                FROM pgstatindex('idx_mvcc_indexed')
            """, "인덱스 통계 (REINDEX 후)")
        except:
            pass

        print("""
★ 핵심 정리:
  1. Index bloat은 MVCC의 자연스러운 부산물
  2. pgstattuple로 bloat 수준 측정 가능
  3. leaf_density < 70%면 REINDEX 권장
  4. REINDEX CONCURRENTLY로 운영 중 재구축 가능
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 4: 테이블 크기와 구성 분석
# =============================================================================

def scenario_4_table_analysis():
    """
    시나리오 4: 테이블 크기와 구성 분석

    테이블별 크기, dead tuple 비율 등을 분석합니다.
    """
    print_section("시나리오 4: 테이블 크기와 구성 분석")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # 4-1: 테이블 크기 현황
        print_subsection("4-1: 테이블 크기 현황")

        size_data = execute_and_show(cur, """
            SELECT
                relname as table_name,
                pg_size_pretty(pg_table_size(c.oid)) as table_size,
                pg_size_pretty(pg_indexes_size(c.oid)) as indexes_size,
                pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
                (SELECT count(*) FROM pg_index WHERE indrelid = c.oid) as index_count
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relkind = 'r'
            ORDER BY pg_total_relation_size(c.oid) DESC
        """, "테이블별 크기")

        # 4-2: Dead Tuple 비율
        print_subsection("4-2: Dead Tuple 비율")

        dead_data = execute_and_show(cur, """
            SELECT
                relname as table_name,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                CASE
                    WHEN n_live_tup + n_dead_tup = 0 THEN 0
                    ELSE ROUND(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
                END as dead_ratio_pct,
                last_vacuum,
                last_autovacuum
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
            ORDER BY n_dead_tup DESC
        """, "Dead Tuple 현황")

        # 4-3: 시각화 - 테이블 구성
        print_subsection("4-3: 테이블 크기 구성 (Graph)")

        cur.execute("""
            SELECT
                relname,
                pg_table_size(c.oid) as table_bytes,
                pg_indexes_size(c.oid) as index_bytes
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relkind = 'r'
            AND pg_total_relation_size(c.oid) > 0
            ORDER BY pg_total_relation_size(c.oid) DESC
            LIMIT 10
        """)
        table_sizes = cur.fetchall()

        if table_sizes:
            fig, ax = plt.subplots(figsize=(12, 6))

            tables = [r[0] for r in table_sizes]
            data_sizes = [r[1] / 1024 for r in table_sizes]  # KB
            index_sizes = [r[2] / 1024 for r in table_sizes]  # KB

            x = np.arange(len(tables))
            width = 0.35

            bars1 = ax.bar(x - width/2, data_sizes, width, label='Table Data', color='#3498db')
            bars2 = ax.bar(x + width/2, index_sizes, width, label='Indexes', color='#e74c3c')

            ax.set_xlabel('Table', fontsize=12)
            ax.set_ylabel('Size (KB)', fontsize=12)
            ax.set_title('Table Data vs Index Size', fontsize=14, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(tables, rotation=45, ha='right')
            ax.legend()

            plt.tight_layout()
            save_graph(fig, 'table_size_composition.png')

        # 4-4: VACUUM 권장 테이블
        print_subsection("4-4: VACUUM 권장 테이블")

        execute_and_show(cur, """
            SELECT
                relname as table_name,
                n_dead_tup as dead_tuples,
                ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_pct,
                pg_size_pretty(pg_total_relation_size(relid)) as total_size,
                CASE
                    WHEN n_dead_tup > 10000 THEN 'HIGH'
                    WHEN n_dead_tup > 1000 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as vacuum_priority
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
            AND n_dead_tup > 100
            ORDER BY n_dead_tup DESC
        """, "VACUUM 우선순위")

        print("""
★ 핵심 정리:
  1. 테이블 크기 = 데이터 + 인덱스 + TOAST
  2. dead_tuple 비율 5% 이상이면 VACUUM 권장
  3. autovacuum이 잘 동작하는지 last_autovacuum 확인
  4. 인덱스 크기가 데이터보다 크면 인덱스 과다
        """)

    finally:
        cur.close()
        conn.close()


# =============================================================================
# 시나리오 5: 종합 성능 대시보드
# =============================================================================

def scenario_5_dashboard():
    """
    시나리오 5: 종합 성능 대시보드

    matplotlib로 종합 성능 지표를 시각화합니다.
    """
    print_section("시나리오 5: 종합 성능 대시보드")

    conn = get_connection()
    cur = conn.cursor()

    try:
        print("성능 지표 수집 중...")

        # 데이터 수집
        # 1. 테이블 크기
        cur.execute("""
            SELECT relname, pg_total_relation_size(c.oid)
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relkind = 'r'
            ORDER BY 2 DESC LIMIT 5
        """)
        top_tables = cur.fetchall()

        # 2. 인덱스 사용률
        cur.execute("""
            SELECT indexrelname, idx_scan
            FROM pg_stat_user_indexes
            WHERE schemaname = 'public'
            ORDER BY idx_scan DESC LIMIT 5
        """)
        top_indexes = cur.fetchall()

        # 3. Dead tuple
        cur.execute("""
            SELECT relname, n_dead_tup, n_live_tup
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
            AND n_live_tup > 0
            ORDER BY n_dead_tup DESC LIMIT 5
        """)
        dead_tuples = cur.fetchall()

        # 4. 캐시 히트율
        cur.execute("""
            SELECT
                SUM(heap_blks_hit) as hit,
                SUM(heap_blks_read) as read
            FROM pg_statio_user_tables
        """)
        cache = cur.fetchone()
        hit_rate = 100 * cache[0] / max(cache[0] + cache[1], 1)

        # 대시보드 생성
        fig = plt.figure(figsize=(16, 12))
        fig.suptitle('PostgreSQL Performance Dashboard', fontsize=16, fontweight='bold', y=0.98)

        # 1. 테이블 크기 (상단 좌)
        ax1 = fig.add_subplot(2, 2, 1)
        if top_tables:
            names = [r[0][:15] for r in top_tables]
            sizes = [r[1] / (1024*1024) for r in top_tables]  # MB
            colors = plt.cm.Blues(np.linspace(0.4, 0.8, len(names)))
            bars = ax1.barh(names, sizes, color=colors, edgecolor='black')
            ax1.set_xlabel('Size (MB)')
            ax1.set_title('Top 5 Tables by Size', fontweight='bold')
            for bar, size in zip(bars, sizes):
                ax1.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                        f'{size:.1f} MB', va='center', fontsize=9)

        # 2. 인덱스 스캔 (상단 우)
        ax2 = fig.add_subplot(2, 2, 2)
        if top_indexes:
            names = [r[0][:20] for r in top_indexes]
            scans = [r[1] for r in top_indexes]
            colors = plt.cm.Greens(np.linspace(0.4, 0.8, len(names)))
            bars = ax2.barh(names, scans, color=colors, edgecolor='black')
            ax2.set_xlabel('Scan Count')
            ax2.set_title('Top 5 Indexes by Usage', fontweight='bold')
            for bar, scan in zip(bars, scans):
                ax2.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                        str(scan), va='center', fontsize=9)

        # 3. Dead Tuple 비율 (하단 좌)
        ax3 = fig.add_subplot(2, 2, 3)
        if dead_tuples:
            names = [r[0][:15] for r in dead_tuples]
            dead_pcts = [100 * r[1] / max(r[1] + r[2], 1) for r in dead_tuples]
            colors = ['#e74c3c' if p > 10 else '#f1c40f' if p > 5 else '#2ecc71' for p in dead_pcts]
            bars = ax3.bar(names, dead_pcts, color=colors, edgecolor='black')
            ax3.set_ylabel('Dead Tuple %')
            ax3.set_title('Dead Tuple Ratio by Table\n(Red>10%, Yellow>5%, Green<5%)', fontweight='bold')
            ax3.axhline(y=5, color='orange', linestyle='--', alpha=0.7)
            ax3.axhline(y=10, color='red', linestyle='--', alpha=0.7)
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # 4. 캐시 히트율 게이지 (하단 우)
        ax4 = fig.add_subplot(2, 2, 4)
        # 도넛 차트로 히트율 표시
        sizes_pie = [hit_rate, 100 - hit_rate]
        colors_pie = ['#2ecc71', '#ecf0f1']
        wedges, texts = ax4.pie(sizes_pie, colors=colors_pie,
                                startangle=90, counterclock=False,
                                wedgeprops=dict(width=0.4, edgecolor='black'))
        ax4.text(0, 0, f'{hit_rate:.1f}%', ha='center', va='center',
                fontsize=24, fontweight='bold')
        ax4.set_title('Buffer Cache Hit Rate\n(Target: >90%)', fontweight='bold')

        plt.tight_layout(rect=[0, 0, 1, 0.96])
        save_graph(fig, 'performance_dashboard.png')

        print("""
┌─────────────────────────────────────────────────────────────────┐
│ 대시보드 생성 완료!                                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ 저장 위치: labs/graphs/performance_dashboard.png                 │
│                                                                  │
│ 포함된 지표:                                                     │
│   1. Top 5 테이블 크기                                           │
│   2. Top 5 인덱스 사용량                                         │
│   3. 테이블별 Dead Tuple 비율                                    │
│   4. 버퍼 캐시 히트율                                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
        """)

        print(f"\n버퍼 캐시 히트율: {hit_rate:.1f}%")
        if hit_rate < 90:
            print("⚠️  캐시 히트율이 90% 미만입니다. shared_buffers 증가를 고려하세요.")
        else:
            print("✓ 캐시 히트율이 양호합니다.")

        print("""
★ 핵심 정리:
  1. 정기적인 성능 모니터링으로 문제 조기 발견
  2. 캐시 히트율 90% 이상 유지 목표
  3. Dead tuple 비율 5% 이하 유지
  4. 미사용 인덱스 정리로 쓰기 성능 개선
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
║          Lab 10: 성능 모니터링과 튜닝                             ║
║          Performance Monitoring & Tuning                         ║
╚══════════════════════════════════════════════════════════════════╝

이 실습에서는 PostgreSQL 성능을 모니터링하고 시각화합니다.

시나리오 목록:
  1. pg_stat_statements - 쿼리 성능 분석
  2. pg_stat_user_indexes - 인덱스 활용도 분석
  3. pgstattuple - Index Bloat 감지
  4. 테이블 크기와 구성 분석
  5. 종합 성능 대시보드 (matplotlib)

그래프 저장 위치: labs/graphs/

실행할 시나리오 번호를 입력하세요 (1-5, 또는 'all'):
    """)

    scenarios = {
        '1': scenario_1_query_stats,
        '2': scenario_2_index_usage,
        '3': scenario_3_index_bloat,
        '4': scenario_4_table_analysis,
        '5': scenario_5_dashboard,
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
