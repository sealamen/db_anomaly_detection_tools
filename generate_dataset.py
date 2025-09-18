import pandas as pd
import numpy as np

'''
설명 : 샘플 데이터를 생성하는 도구 
- start_time ~ end_time 까지의 샘플 데이터를 10초 단위로 생성 
- 시간, 요일을 고려하여 생성 
- 실행 완료 시, /datasets/csv 폴더에 샘플 데이터 생성

실행 방법 
1.  start_time 설정 : 해당 날짜부터 데이터를 생성 (하루에 10000건 조금 안됨)
2.  save_file_name 설정 : 설정된 이름으로 데이터 생성 

'''

save_file_name = 'synthetic_db_metrics_2025_09_17_test.csv'

# =============================
# 1️⃣ 시간 범위
# =============================
# start_time = pd.Timestamp('2025-09-01 00:00:00')
start_time = pd.Timestamp('2025-09-17 00:00:00')
end_time = pd.Timestamp.now()
freq_sec = 10
ts_index = pd.date_range(start=start_time, end=end_time, freq=f'{freq_sec}S')
N = len(ts_index)

# 시간/요일 정보 추출
hours = ts_index.hour
weekdays = ts_index.weekday  # 월=0, 일=6

# =============================
# 2️⃣ CPU / Active Sessions 패턴 생성
# =============================
host_cpu_util_pct = np.where(
    (hours >= 9) & (hours < 18) & (weekdays < 5),
    np.random.uniform(30, 50, N),   # 업무시간 CPU 높음
    np.random.uniform(5, 20, N)     # 야간/주말 CPU 낮음
)

active_sessions = np.where(
    (hours >= 9) & (hours < 18) & (weekdays < 5),
    np.random.randint(80, 150, N),
    np.random.randint(10, 50, N)
)

# =============================
# 3️⃣ 나머지 컬럼 생성
# =============================
data = pd.DataFrame({
    'time': ts_index,
    'host_cpu_util_pct': host_cpu_util_pct,
    'host_cpu_usage_per_sec': np.random.uniform(0, 5, N),
    'db_cpu_time_ratio': np.random.uniform(0, 0.5, N),
    'db_cpu_usage_per_sec': np.random.uniform(0, 5, N),
    'cpu_usage_per_txn': np.random.uniform(0.01, 0.1, N),
    'bg_cpu_usage_per_sec': np.random.uniform(0, 2, N),
    'buffer_cache_hit_ratio': np.random.uniform(85, 99, N),
    'shared_pool_free_pct': np.random.uniform(15, 50, N),
    'library_cache_hit_ratio': np.random.uniform(85, 99, N),
    'sga_free_mb': np.random.uniform(200, 1000, N),
    'pga_used_mb': np.random.uniform(50, 500, N),
    'active_sessions': active_sessions,
    'sessions_total': np.random.randint(50, 300, N),
    'logons_per_sec': np.random.uniform(0, 2, N),
    'process_count': np.random.randint(50, 400, N),
    'physical_reads_per_sec': np.random.uniform(10, 300, N),
    'physical_writes_per_sec': np.random.uniform(10, 300, N),
    'redo_writes_per_sec': np.random.uniform(5, 200, N),
    'io_requests_per_sec': np.random.uniform(20, 1000, N),
    'io_throughput_mb_sec': np.random.uniform(0.5, 50, N),
    'avg_read_latency_ms': np.random.uniform(1, 20, N),
    'avg_write_latency_ms': np.random.uniform(1, 20, N),
    'db_time_ms': np.random.uniform(50, 1000, N),
    'cpu_time_ms': np.random.uniform(10, 500, N),
    'user_io_wait_ms': np.random.uniform(5, 200, N),
    'system_io_wait_ms': np.random.uniform(5, 200, N),
    'log_file_sync_wait_ms': np.random.uniform(1, 50, N),
    'concurrency_wait_ms': np.random.uniform(0, 100, N),
    'txn_per_sec': np.random.uniform(0, 50, N),
    'user_calls_per_sec': np.random.uniform(0, 100, N),
    'executions_per_sec': np.random.uniform(0, 200, N),
    'parse_count_per_sec': np.random.uniform(0, 50, N),
    'hard_parse_ratio_pct': np.random.uniform(0, 20, N)
})

# =============================
# 4️⃣ CSV 저장
# =============================

data.to_csv('./datasets/csv/' + save_file_name, index=False)
print(f"✅ 시간대/요일 패턴 반영 정상 데이터 생성 완료! 총 {N}건 -> {save_file_name}")

