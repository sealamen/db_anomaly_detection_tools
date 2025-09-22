import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def visualize_db_metrics(csv_file, metrics=None, show_anomaly=True):
    """
    DB 성능 지표 CSV 시각화
    ---------------------------------
    - csv_file: 생성된 CSV 파일 경로
    - metrics: 시각화할 지표 리스트 (기본: 핵심 지표)
    - show_anomaly: True이면 이상치(is_anomaly=1) 표시
    """
    # CSV 읽기
    data = pd.read_csv(csv_file, parse_dates=['time'])

    # 기본 시각화 지표
    if metrics is None:
        metrics = [
            "db_time_ms",
            "db_cpu_usage_per_sec",
            "host_cpu_usage_per_sec",
            "physical_reads_per_sec",
            "physical_writes_per_sec",
            "redo_writes_per_sec",
            "txn_per_sec",
            "commits_per_sec",
            "rollbacks_per_sec",
            "executions_per_sec",
            "aas",
            "logons_per_sec",
            "enqueue_waits_per_sec",
            "sql_service_response_time"
        ]

    # 시간대별 라인 그래프
    plt.figure(figsize=(16, len(metrics) * 2))
    for i, metric in enumerate(metrics):
        plt.subplot(len(metrics), 1, i + 1)
        plt.plot(data['time'], data[metric], label=metric, color='blue', alpha=0.7)

        if show_anomaly and 'is_anomaly' in data.columns:
            anomaly_points = data[data['is_anomaly'] == 1]
            plt.scatter(anomaly_points['time'], anomaly_points[metric],
                        color='red', label='anomaly', s=10)

        plt.ylabel(metric)
        if i == 0:
            plt.title("DB 성능 지표 시계열 (이상치 붉은색 표시)")
        if i == len(metrics) - 1:
            plt.xlabel("Time")
        plt.legend(loc='upper right')
    plt.tight_layout()
    plt.show()

    # 요약 통계
    summary = data[metrics].describe()
    print("\n===== 요약 통계 =====")
    print(summary)

    # 이상치 비율
    if 'is_anomaly' in data.columns:
        anomaly_ratio = data['is_anomaly'].mean()
        print(f"\n총 이상치 비율: {anomaly_ratio * 100:.2f}%")

    return data


def summarize_db_metrics(csv_file, metrics=None):
    """
    DB 성능 지표 요약 시각화
    ---------------------------------
    - csv_file: 생성된 CSV 파일 경로
    - metrics: 분석할 지표 리스트 (기본: 핵심 지표)
    """
    data = pd.read_csv(csv_file)

    # 기본 지표
    if metrics is None:
        metrics = [
            "db_time_ms",
            "db_cpu_usage_per_sec",
            "host_cpu_usage_per_sec",
            "physical_reads_per_sec",
            "physical_writes_per_sec",
            "redo_writes_per_sec",
            "txn_per_sec",
            "commits_per_sec",
            "rollbacks_per_sec",
            "executions_per_sec",
            "aas",
            "logons_per_sec",
            "enqueue_waits_per_sec",
            "sql_service_response_time"
        ]

    n_metrics = len(metrics)
    n_cols = 3
    n_rows = (n_metrics + n_cols - 1) // n_cols

    plt.figure(figsize=(n_cols * 5, n_rows * 3))

    for i, metric in enumerate(metrics):
        plt.subplot(n_rows, n_cols, i + 1)
        sns.histplot(data[metric], kde=True, bins=30, color='skyblue')
        plt.title(metric)
        plt.xlabel("")
        plt.ylabel("")
        # 이상치 비율 표시
        if 'is_anomaly' in data.columns:
            anomaly_ratio = data.loc[data['is_anomaly'] == 1, metric].count() / len(data)
            plt.text(0.95, 0.95, f"{anomaly_ratio * 100:.2f}% anomaly",
                     horizontalalignment='right', verticalalignment='top',
                     transform=plt.gca().transAxes, fontsize=9, color='red')

    plt.tight_layout()
    plt.show()

    # 요약 통계
    summary = data[metrics].describe().T
    if 'is_anomaly' in data.columns:
        summary['anomaly_ratio'] = data[metrics].apply(lambda x: (data['is_anomaly'] == 1).sum() / len(data))
    print("\n===== 요약 통계 =====")
    print(summary)

    return data


def visualize_anomalies(csv_file, metrics=None):
    """
    DB 성능 지표 분포 + 이상치 시각화
    ---------------------------------
    - csv_file: 생성된 CSV 파일 경로
    - metrics: 분석할 지표 리스트 (기본: 핵심 지표)
    """
    data = pd.read_csv(csv_file, parse_dates=['time'])

    if metrics is None:
        metrics = [
            "db_time_ms",
            "db_cpu_usage_per_sec",
            "host_cpu_usage_per_sec",
            "physical_reads_per_sec",
            "physical_writes_per_sec",
            "redo_writes_per_sec",
            "txn_per_sec",
            "commits_per_sec",
            "rollbacks_per_sec",
            "executions_per_sec",
            "aas",
            "logons_per_sec",
            "enqueue_waits_per_sec",
            "sql_service_response_time"
        ]

    n_metrics = len(metrics)
    n_cols = 3
    n_rows = (n_metrics + n_cols - 1) // n_cols

    plt.figure(figsize=(n_cols * 5, n_rows * 3))

    for i, metric in enumerate(metrics):
        plt.subplot(n_rows, n_cols, i + 1)
        # 정상 데이터
        sns.histplot(data.loc[data['is_anomaly'] == 0, metric], bins=30, color='skyblue', kde=True, label='normal')
        # 이상치
        if 'is_anomaly' in data.columns:
            sns.histplot(data.loc[data['is_anomaly'] == 1, metric], bins=30, color='red', kde=False, label='anomaly')
            anomaly_ratio = data['is_anomaly'].mean()
            plt.title(f"{metric} (anomaly {anomaly_ratio * 100:.2f}%)")
        else:
            plt.title(metric)

        plt.xlabel("")
        plt.ylabel("")
        plt.legend()

    plt.tight_layout()
    plt.show()

    # 기본 통계
    summary = data[metrics].describe().T
    if 'is_anomaly' in data.columns:
        summary['anomaly_ratio'] = data[metrics].apply(lambda x: (data['is_anomaly'] == 1).sum() / len(data))
    print("\n===== 요약 통계 =====")
    print(summary)

    return data


csv_file = "./datasets/csv/anomaly_txn.csv"
df = visualize_anomalies(csv_file)
