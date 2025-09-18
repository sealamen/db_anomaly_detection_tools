import pandas as pd

'''
기존 csv 파일을 sql 파일로 전환하는 도구 
- 대량의 데이터의 경우 insert 시 부하가 심하므로, 1000건 단위로 insertAll, commit 을 진행

실행 방법 
1. import_file_name 지정 : 가져올 csv 파일 이름 
2. save_file_name 지정 : 생성할 sql 파일 이름 

'''

import_file_name = 'synthetic_db_metrics_2025_09_17_test.csv'
save_file_name = 'insert_db_metrics_batch_2025_09_17_test.sql'


# =============================
# 1️⃣ CSV 읽기
# =============================
csv_file = './datasets/csv/' + import_file_name
df = pd.read_csv(csv_file)

# =============================
# 2️⃣ 테이블명 설정
# =============================
# table_name = 'DB_METRICS'
table_name = 'DB_PERF_LOG'
batch_size = 1000  # 한 번에 몇 행씩 INSERT 할지

# =============================
# 3️⃣ 배치 INSERT 생성
# =============================
sql_file = './datasets/sql/' + save_file_name
with open(sql_file, 'w') as f:
    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch = df.iloc[start:end]

        f.write(f"INSERT ALL\n")
        for _, row in batch.iterrows():
            values = []
            for col in df.columns:
                val = row[col]
                if pd.isna(val):
                    values.append('NULL')
                elif df[col].dtype == 'object':
                    if col == 'time':
                        values.append(f"TO_TIMESTAMP('{val}', 'YYYY-MM-DD HH24:MI:SS')")
                    else:
                        values.append(f"'{val}'")
                else:
                    values.append(str(val))
            values_str = ', '.join(values)
            f.write(f"  INTO {table_name} ({', '.join(df.columns)}) VALUES ({values_str})\n")
        f.write("SELECT * FROM dual;\n\n")

print(f"✅ 배치 INSERT SQL 파일 생성 완료! -> '{sql_file}'")
