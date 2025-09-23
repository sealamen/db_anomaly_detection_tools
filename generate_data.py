import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns

def generate_synthetic_metrics(start_time, end_time, freq_sec=10,
                               anomaly_ratio=0.02,
                               scenario="default",
                               save_file_name=None, include_is_anomaly=False):
    """
    현실적인 DB 성능 지표 샘플 데이터 생성기
    -----------------------------------------
    - anomaly_ratio: 이상치 비율 (0 ~ 1)
    - scenario: 이상치 유형
        "default" -> 랜덤 이상치
        "cpu_surge" -> CPU/세션 폭주
        "txn_bottleneck" -> 트랜잭션 폭주
        "io_bottleneck" -> I/O 지연
        "lock_wait" -> Lock/Deadlock
        "logon_spike" -> 커넥션 폭주
    - include_is_anomaly: True면 is_anomaly 컬럼 포함
    """

    np.random.seed(42)

    # 시간 인덱스
    ts_index = pd.date_range(start=start_time, end=end_time, freq=f'{freq_sec}S')
    N = len(ts_index)

    # 정상 범위 정의
    ranges = {
        "CPU_USAGE_PER_SEC": (0, 5),
        "HOST_CPU_USAGE_PER_SEC": (5, 50),
        "PHYSICAL_READS_PER_SEC": (10, 300),
        "PHYSICAL_WRITES_PER_SEC": (10, 300),
        "IO_MB_PER_SEC": (1, 100),
        "REDO_GENERATED_PER_SEC": (5, 200),
        "DB_BLOCK_CHANGES_PER_SEC": (5, 200),
        "CONSISTENT_READ_GETS_PER_SEC": (10, 500),
        "LOGICAL_READS_PER_SEC": (10, 500),
        "DBWR_CHECKPOINTS_PER_SEC": (0, 50),
        "EXECUTIONS_PER_SEC": (0, 200),
        "HARD_PARSE_COUNT_PER_SEC": (0, 20),
        "DB_TIME_PER_SEC": (50, 1000),
        "AVG_ACTIVE_SESSIONS": (1, 16),
        "LOGONS_PER_SEC": (0, 10),
        "USER_CALLS_PER_SEC": (0, 100),
        "USER_COMMITS_PER_SEC": (0, 50),
        "USER_ROLLBACKS_PER_SEC": (0, 5),
        "ENQUEUE_WAITS_PER_SEC": (0, 5)
    }

    # 평균/표준편차 정의
    metrics_mean_std = {
        "CPU_USAGE_PER_SEC": (2.5, 1),
        "HOST_CPU_USAGE_PER_SEC": (25, 10),
        "PHYSICAL_READS_PER_SEC": (150, 50),
        "PHYSICAL_WRITES_PER_SEC": (150, 50),
        "IO_MB_PER_SEC": (50, 20),
        "REDO_GENERATED_PER_SEC": (100, 50),
        "DB_BLOCK_CHANGES_PER_SEC": (100, 50),
        "CONSISTENT_READ_GETS_PER_SEC": (250, 100),
        "LOGICAL_READS_PER_SEC": (250, 100),
        "DBWR_CHECKPOINTS_PER_SEC": (10, 5),
        "EXECUTIONS_PER_SEC": (100, 50),
        "HARD_PARSE_COUNT_PER_SEC": (10, 5),
        "DB_TIME_PER_SEC": (500, 100),
        "AVG_ACTIVE_SESSIONS": (8, 3),
        "LOGONS_PER_SEC": (5, 2),
        "USER_CALLS_PER_SEC": (50, 30),
        "USER_COMMITS_PER_SEC": (25, 15),
        "USER_ROLLBACKS_PER_SEC": (2, 1),
        "ENQUEUE_WAITS_PER_SEC": (1, 0.5)
    }

    # 정상 데이터 생성
    data = pd.DataFrame({"time": ts_index})

    # 시간대 및 요일 패턴 생성
    hour = data["time"].dt.hour
    weekday = data["time"].dt.weekday  # 0=Mon, 6=Sun
    hour_weight = 0.5 + 0.5*np.sin((hour/24)*2*np.pi)  # 패턴 적용 가중치 : 낮 시간대 상승
    weekday_weight = np.where(weekday < 5, 1.0, 0.7)   # 패턴 적용 가중치 : 주말 낮음

    for metric, (mean, std) in metrics_mean_std.items():
        base = np.random.normal(mean, std, N)
        base = base * hour_weight * weekday_weight
        base = np.clip(base, 0, None) # 음수 제거
        data[metric] = base

    # 지표 간 상관관계 적용 (단순 선형 비례)
    data["DB_TIME_PER_SEC"] += data["CPU_USAGE_PER_SEC"]*50 + data["HOST_CPU_USAGE_PER_SEC"]*10
    data["REDO_GENERATED_PER_SEC"] += data["USER_CALLS_PER_SEC"]*5
    data["USER_COMMITS_PER_SEC"] += data["USER_CALLS_PER_SEC"]*0.5
    data["DB_BLOCK_CHANGES_PER_SEC"] += data["PHYSICAL_WRITES_PER_SEC"]*0.2
    data["EXECUTIONS_PER_SEC"] += data["USER_CALLS_PER_SEC"]*0.8
    data["AVG_ACTIVE_SESSIONS"] += data["USER_CALLS_PER_SEC"]*0.05 + data["CPU_USAGE_PER_SEC"]*0.1
    data["DB_TIME_PER_SEC"] += (data["PHYSICAL_READS_PER_SEC"] + data["PHYSICAL_WRITES_PER_SEC"])*0.1

    # is_anomaly 컬럼 초기화
    data["ANOMALY_YN"] = "N"  # 기본값 N

    # 이상치 생성
    if anomaly_ratio > 0:
        n_anomalies = max(1, int(N * anomaly_ratio))
        anomaly_idx = np.random.choice(N, size=n_anomalies, replace=False)

        for idx in anomaly_idx:
            if scenario == "default":
                # 랜덤 이상치: 전체 지표 중 일부 30% 정도만 범위 벗어남
                for metric, (low, high) in ranges.items():
                    if np.random.rand() < 0.3:
                        if np.random.rand() < 0.5:
                            data.loc[idx, metric] = high * (1.5 + np.random.rand())
                        else:
                            data.loc[idx, metric] = low * (0.1 + 0.2 * np.random.rand())
            else:
                # 시나리오 기반 이상치: 관련 지표 동시에 폭주
                if scenario == "cpu_surge":
                    data.loc[idx, "CPU_USAGE_PER_SEC"] = np.random.uniform(10, 20)
                    data.loc[idx, "HOST_CPU_USAGE_PER_SEC"] = np.random.uniform(90, 100)
                    data.loc[idx, "AVG_ACTIVE_SESSIONS"] = np.random.randint(50, 150)
                    data.loc[idx, "DB_TIME_PER_SEC"] = np.random.uniform(1500, 3000)
                elif scenario == "txn_bottleneck":
                    data.loc[idx, "USER_CALLS_PER_SEC"] = np.random.uniform(100, 500)
                    data.loc[idx, "USER_COMMITS_PER_SEC"] = np.random.uniform(50, 200)
                    data.loc[idx, "USER_ROLLBACKS_PER_SEC"] = np.random.uniform(5, 50)
                    data.loc[idx, "REDO_GENERATED_PER_SEC"] = np.random.uniform(300, 1000)
                    data.loc[idx, "DB_TIME_PER_SEC"] = np.random.uniform(1500, 3000)
                elif scenario == "io_bottleneck":
                    data.loc[idx, "PHYSICAL_READS_PER_SEC"] = np.random.uniform(500, 2000)
                    data.loc[idx, "PHYSICAL_WRITES_PER_SEC"] = np.random.uniform(500, 2000)
                    data.loc[idx, "IO_MB_PER_SEC"] = np.random.uniform(100, 500)
                    data.loc[idx, "DBWR_CHECKPOINTS_PER_SEC"] = np.random.uniform(20, 100)
                    data.loc[idx, "DB_TIME_PER_SEC"] = np.random.uniform(1500, 3000)
                elif scenario == "lock_wait":
                    data.loc[idx, "ENQUEUE_WAITS_PER_SEC"] = np.random.uniform(5, 50)
                    data.loc[idx, "USER_ROLLBACKS_PER_SEC"] = np.random.uniform(5, 50)
                elif scenario == "logon_spike":
                    data.loc[idx, "LOGONS_PER_SEC"] = np.random.randint(20, 100)
                    data.loc[idx, "AVG_ACTIVE_SESSIONS"] = np.random.randint(50, 150)
                    data.loc[idx, "USER_CALLS_PER_SEC"] = np.random.uniform(100, 500)

            # 이상치 표시
            data.loc[idx, "ANOMALY_YN"] = "Y"

    # 파일 저장
    if save_file_name:
        dir_path = os.path.dirname(save_file_name)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        if include_is_anomaly:
            data.to_csv(save_file_name, index=False)
        else:
            data.drop(columns="ANOMALY_YN").to_csv(save_file_name, index=False)

        print(f"✅ 데이터 생성 완료! 총 {N}건 중 {data[data['ANOMALY_YN'] == 'Y'].shape[0]}건 이상치 "
              f"-> {save_file_name} (label 저장: {include_is_anomaly})")

    return data

# CPU 폭주 시나리오
# df_cpu = generate_synthetic_metrics(
#     start_time="2025-09-20",
#     end_time=pd.Timestamp.now(),
#     freq_sec=10,
#     anomaly_ratio=0.1,
#     save_file_name="./datasets/csv/anomaly_txn.csv",
#     scenario="txn_bottleneck"   # cpu_surge, txn_bottleneck, io_bottleneck, lock_wait, logon_spike
# )

# 트랜잭션 이상
# df_txn = generate_synthetic_metrics(
#     start_time="2025-09-20",
#     end_time=pd.Timestamp.now(),
#     freq_sec=10,
#     anomaly_ratio=0.1,
#     save_file_name="./datasets/csv/anomaly_txn_tmp.csv",
#     scenario="txn_bottleneck",    # cpu_surge, txn_bottleneck, io_bottleneck, lock_wait, logon_spike
#     include_is_anomaly=False
# )

# 100% 정상 데이터
# df_normal = generate_synthetic_metrics(
#     start_time="2025-09-20",
#     end_time=pd.Timestamp.now(),
#     anomaly_ratio=0,
#     save_file_name="./datasets/csv/normal_0920_.csv",
#     scenario='default', # cpu_surge, txn_bottleneck, io_bottleneck, lock_wait, logon_spike
#     include_is_anomaly=False
# )

# 이상치 0.02
df_normal = generate_synthetic_metrics(
    start_time="2025-09-24",
    end_time="2025-09-25 00:00:00",
    anomaly_ratio=0.02,
    save_file_name="./datasets/csv/anomaly_0924_.csv",
    scenario='default', # cpu_surge, txn_bottleneck, io_bottleneck, lock_wait, logon_spike
    include_is_anomaly=False
)

