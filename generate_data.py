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
    현실적인 DB 성능 지표 샘플 데이터 생성기 (고급 시나리오 이상치 포함)
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

    # 정상 범위 정의 (19개 지표)
    ranges = {
        "db_time_ms": (50, 1000),
        "db_cpu_usage_per_sec": (0, 5),
        "host_cpu_usage_per_sec": (5, 50),
        "physical_reads_per_sec": (10, 300),
        "physical_writes_per_sec": (10, 300),
        "redo_writes_per_sec": (5, 200),
        "user_calls_per_sec": (0, 100),
        "txn_per_sec": (0, 50),
        "commits_per_sec": (0, 50),
        "rollbacks_per_sec": (0, 5),
        "executions_per_sec": (0, 200),
        "hard_parse_count_per_sec": (0, 20),
        "aas": (1, 16),
        "logons_per_sec": (0, 10),
        "parse_count_per_sec": (0, 50),
        "enqueue_waits_per_sec": (0, 2),
        "host_cpu_util_pct": (20, 70),
        "db_cpu_time_ratio": (0, 0.5),
        "sql_service_response_time": (1, 200)
    }

    # 기본 정상 평균/표준편차
    metrics_mean_std = {
        "db_time_ms": (500, 100),
        "db_cpu_usage_per_sec": (2.5, 1),
        "host_cpu_usage_per_sec": (25, 10),
        "physical_reads_per_sec": (150, 50),
        "physical_writes_per_sec": (150, 50),
        "redo_writes_per_sec": (100, 50),
        "user_calls_per_sec": (50, 30),
        "txn_per_sec": (25, 15),
        "commits_per_sec": (25, 15),
        "rollbacks_per_sec": (2, 1),
        "executions_per_sec": (100, 50),
        "hard_parse_count_per_sec": (10, 5),
        "aas": (8, 3),
        "logons_per_sec": (5, 2),
        "parse_count_per_sec": (25, 10),
        "enqueue_waits_per_sec": (1, 0.5),
        "host_cpu_util_pct": (45, 15),
        "db_cpu_time_ratio": (0.25, 0.1),
        "sql_service_response_time": (100, 50)
    }

    # 정상 데이터 생성
    data = pd.DataFrame({"time": ts_index})

    # 시간대 및 요일 패턴 생성
    hour = data["time"].dt.hour
    weekday = data["time"].dt.weekday  # 0=Mon, 6=Sun

    # 패턴 적용 가중치
    hour_weight = 0.5 + 0.5*np.sin((hour/24)*2*np.pi)   # 낮 시간대 상승
    weekday_weight = np.where(weekday < 5, 1.0, 0.7)     # 주말 낮음

    for metric, (mean, std) in metrics_mean_std.items():
        base = np.random.normal(mean, std, N)
        base = base * hour_weight * weekday_weight
        base = np.clip(base, 0, None) # 음수 제거
        data[metric] = base

    # 지표 간 상관관계 적용 (단순 선형 비례)
    # DB CPU ↑ → DB Time ↑, DB CPU Ratio ↑
    data["db_time_ms"] += data["db_cpu_usage_per_sec"] * 50
    data["db_cpu_time_ratio"] = np.clip(data["db_cpu_usage_per_sec"] / np.maximum(data["host_cpu_usage_per_sec"], 1), 0, 1)

    # Transactions ↑ → Redo, Commits ↑
    data["redo_writes_per_sec"] += data["txn_per_sec"] * 5
    data["commits_per_sec"] += data["txn_per_sec"] * 0.5

    # Physical Reads/Writes ↑ → SQL Service Response Time ↑
    data["sql_service_response_time"] += (data["physical_reads_per_sec"] + data["physical_writes_per_sec"]) * 0.1

    # User Calls ↑ → Executions ↑
    data["executions_per_sec"] += data["user_calls_per_sec"] * 0.8


    # =============================
    # is_anomaly 컬럼 초기화
    # =============================
    data["is_anomaly"] = 0

    # =============================
    # 이상치 생성
    # =============================
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
                    data.loc[idx, "host_cpu_util_pct"] = np.random.uniform(90, 100)
                    data.loc[idx, "db_cpu_usage_per_sec"] = np.random.uniform(10, 20)
                    data.loc[idx, "aas"] = np.random.randint(50, 150)
                    data.loc[idx, "db_time_ms"] = np.random.uniform(1500, 3000)
                elif scenario == "txn_bottleneck":
                    data.loc[idx, "txn_per_sec"] = np.random.uniform(50, 200)
                    data.loc[idx, "commits_per_sec"] = np.random.uniform(50, 200)
                    data.loc[idx, "rollbacks_per_sec"] = np.random.uniform(5, 50)
                    data.loc[idx, "redo_writes_per_sec"] = np.random.uniform(300, 1000)
                    data.loc[idx, "db_time_ms"] = np.random.uniform(1500, 3000)
                elif scenario == "io_bottleneck":
                    data.loc[idx, "physical_reads_per_sec"] = np.random.uniform(500, 2000)
                    data.loc[idx, "physical_writes_per_sec"] = np.random.uniform(500, 2000)
                    data.loc[idx, "redo_writes_per_sec"] = np.random.uniform(300, 1000)
                    data.loc[idx, "sql_service_response_time"] = np.random.uniform(500, 2000)
                    data.loc[idx, "user_calls_per_sec"] = np.random.uniform(200, 500)
                elif scenario == "lock_wait":
                    data.loc[idx, "enqueue_waits_per_sec"] = np.random.uniform(5, 50)
                    data.loc[idx, "rollbacks_per_sec"] = np.random.uniform(5, 50)
                    data.loc[idx, "txn_per_sec"] = np.random.uniform(50, 200)
                elif scenario == "logon_spike":
                    data.loc[idx, "logons_per_sec"] = np.random.randint(20, 100)
                    data.loc[idx, "aas"] = np.random.randint(50, 150)
                    data.loc[idx, "user_calls_per_sec"] = np.random.uniform(100, 500)

            # 이상치 표시
            data.loc[idx, "is_anomaly"] = 1

    # =============================
    # 파일 저장
    # =============================
    if save_file_name:
        dir_path = os.path.dirname(save_file_name)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        if include_is_anomaly:
            data.to_csv(save_file_name, index=False)
        else:
            data.drop(columns="is_anomaly").to_csv(save_file_name, index=False)

        print(f"✅ 데이터 생성 완료! 총 {N}건 중 {data['is_anomaly'].sum()}건 이상치 "
              f"-> {save_file_name} (label 저장: {include_is_anomaly})")

    return data


def summarize_anomalies(data, metrics=None):
    """
    생성된 데이터에서 이상치 통계를 요약
    --------------------------------------
    - data: generate_synthetic_metrics로 생성된 DataFrame
    - metrics: 요약할 지표 리스트 (None이면 19개 주요 지표 모두)
    """

    if metrics is None:
        metrics = [
            "db_time_ms", "db_cpu_usage_per_sec", "host_cpu_usage_per_sec",
            "physical_reads_per_sec", "physical_writes_per_sec", "redo_writes_per_sec",
            "user_calls_per_sec", "txn_per_sec", "commits_per_sec", "rollbacks_per_sec",
            "executions_per_sec", "hard_parse_count_per_sec", "aas", "logons_per_sec",
            "parse_count_per_sec", "enqueue_waits_per_sec", "host_cpu_util_pct",
            "db_cpu_time_ratio", "sql_service_response_time"
        ]

    summary = []

    for metric in metrics:
        total_count = len(data)
        anomaly_count = data.loc[data["is_anomaly"]==1, metric].count()
        normal_count = data.loc[data["is_anomaly"]==0, metric].count()

        anomaly_mean = data.loc[data["is_anomaly"]==1, metric].mean()
        normal_mean = data.loc[data["is_anomaly"]==0, metric].mean()

        anomaly_max = data.loc[data["is_anomaly"]==1, metric].max()
        normal_max = data.loc[data["is_anomaly"]==0, metric].max()

        summary.append({
            "metric": metric,
            "total_count": total_count,
            "anomaly_count": anomaly_count,
            "anomaly_ratio": anomaly_count / total_count,
            "normal_mean": normal_mean,
            "anomaly_mean": anomaly_mean,
            "normal_max": normal_max,
            "anomaly_max": anomaly_max
        })

    summary_df = pd.DataFrame(summary)
    return summary_df


def plot_anomaly_summary(summary_df, title="Anomaly Summary"):
    """
    이상치 요약 통계 시각화
    -----------------------------
    - summary_df: summarize_anomalies로 만든 DataFrame
    - title: 그래프 제목
    """

    metrics = summary_df["metric"]

    # 평균값 비교 (정상 vs 이상치)
    plt.figure(figsize=(15, 6))
    plt.bar(metrics, summary_df["normal_mean"], color="skyblue", label="Normal Mean")
    plt.bar(metrics, summary_df["anomaly_mean"], color="salmon", alpha=0.7, label="Anomaly Mean")
    plt.xticks(rotation=90)
    plt.ylabel("Value")
    plt.title(f"{title} - Mean Comparison")
    plt.legend()
    plt.tight_layout()
    plt.show()

    # 최대값 비교 (정상 vs 이상치)
    plt.figure(figsize=(15, 6))
    plt.bar(metrics, summary_df["normal_max"], color="skyblue", label="Normal Max")
    plt.bar(metrics, summary_df["anomaly_max"], color="salmon", alpha=0.7, label="Anomaly Max")
    plt.xticks(rotation=90)
    plt.ylabel("Value")
    plt.title(f"{title} - Max Comparison")
    plt.legend()
    plt.tight_layout()
    plt.show()

    # 이상치 비율 시각화
    plt.figure(figsize=(15, 4))
    sns.barplot(x=metrics, y=summary_df["anomaly_ratio"], color="salmon")
    plt.xticks(rotation=90)
    plt.ylabel("Anomaly Ratio")
    plt.title(f"{title} - Anomaly Ratio")
    plt.ylim(0, 1)
    plt.tight_layout()
    plt.show()



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
df_normal = generate_synthetic_metrics(
    start_time="2025-09-22 09:00:00",
    end_time=pd.Timestamp.now(),
    anomaly_ratio=0,
    save_file_name="./datasets/csv/normal_split.csv",
    scenario='default', # cpu_surge, txn_bottleneck, io_bottleneck, lock_wait, logon_spike
    include_is_anomaly=False
)

# 이상치 통계 확인
summary_txn = summarize_anomalies(df_normal)
print(summary_txn)

# plot_anomaly_summary(summary_txn, title="Transaction Bottleneck Scenario")
