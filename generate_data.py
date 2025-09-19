import pandas as pd
import numpy as np
import os

def generate_synthetic_metrics(start_time, end_time, freq_sec=10, mode="normal",
                               save_file_name=None, include_is_anomaly=False):
    """
    TODO : 정규분포를 따르는 데이터셋 만들기 (이상치 데이터는 학습 전, 데이터 import 할 때 전처리)
    DB 성능 지표 샘플 데이터 생성기
    mode: "normal" -> 정상 패턴 / "anomaly" -> 이상치 포함
    include_is_anomaly: True면 is_anomaly 컬럼도 CSV에 포함 저장
    """

    np.random.seed(42)

    # =============================
    # 시간 인덱스
    # =============================
    ts_index = pd.date_range(start=start_time, end=end_time, freq=f'{freq_sec}S')
    N = len(ts_index)

    hours = ts_index.hour
    weekdays = ts_index.weekday

    # =============================
    # CPU / 세션 패턴
    # =============================
    host_cpu_util_pct = np.where(
        (hours >= 9) & (hours < 18) & (weekdays < 5),
        np.random.uniform(30, 50, N),
        np.random.uniform(5, 20, N)
    )

    active_sessions = np.where(
        (hours >= 9) & (hours < 18) & (weekdays < 5),
        np.random.randint(80, 150, N),
        np.random.randint(10, 50, N)
    )

    # =============================
    # 기본 정상 데이터
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
        'sessions_total': np.random.randint(50, 300, N),
        'active_sessions': active_sessions,
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
        'hard_parse_ratio_pct': np.random.uniform(0, 20, N),
        'sga_free_mb': np.random.uniform(200, 1000, N),
        'pga_used_mb': np.random.uniform(50, 500, N)
    })

    # =============================
    # 이상치 삽입
    # =============================
    data["is_anomaly"] = 0
    if mode == "anomaly":
        anomaly_idx = np.random.choice(N, size=max(5, N // 50), replace=False)  # 약 2%
        data.loc[anomaly_idx, "host_cpu_util_pct"] = np.random.uniform(90, 100, len(anomaly_idx))
        data.loc[anomaly_idx, "active_sessions"] = np.random.randint(500, 1000, len(anomaly_idx))
        data.loc[anomaly_idx, "avg_read_latency_ms"] = np.random.uniform(100, 500, len(anomaly_idx))
        data.loc[anomaly_idx, "avg_write_latency_ms"] = np.random.uniform(100, 500, len(anomaly_idx))
        data.loc[anomaly_idx, "is_anomaly"] = 1

    # =============================
    # 저장
    # =============================
    if save_file_name:
        dir_path = os.path.dirname(save_file_name)
        if dir_path:  # 경로가 있을 때만 생성
            os.makedirs(dir_path, exist_ok=True)

        if include_is_anomaly:
            data.to_csv(save_file_name, index=False)
        else:
            cols_to_save = [c for c in data.columns if c != "is_anomaly"]
            data[cols_to_save].to_csv(save_file_name, index=False)

        print(f"✅ {mode.upper()} 데이터 생성 완료! 총 {N}건 -> {save_file_name} (label 저장: {include_is_anomaly})")

    return data


# 데이터 생성 (정상 데이터)
generate_synthetic_metrics(start_time="2025-09-16",
                           end_time=pd.Timestamp.now(),
                           mode="normal",  # normal / anomaly
                           save_file_name="./datasets/csv/normal_split.csv",
                           include_is_anomaly=False)  # True / False

# 데이터 생성 (이상 데이터 포함) : 위에꺼 수정하면 되는데 그냥 하나 더 해놓음
# generate_synthetic_metrics(start_time="2025-09-20",
#                            end_time="2025-09-22",
#                            mode="anomaly",
#                            save_file_name="./datasets/csv/anomaly.csv",
#                            include_is_anomaly=False)
