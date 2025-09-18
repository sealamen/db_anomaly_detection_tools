import oracledb
import math

'''
sql insert 파일을 실행하여 DB 에 저장하는 도구 
- csv_to_sql 파일로 생성된 1000건 단위의 insertALL 쿼리 실행만 가능 
- 일반 sql 파일 insert 는 손봐야됨 

실행 방법 
1 ) oracle instant client 경로 설정 : 로컬에 있는 위치 찾아가기 
2 ) 데이터를 insert 할 DB 정보 입력 
3 ) 실행할 SQL 파일 경로 및 이름 입력 

'''

# =============================
# 1️⃣ Oracle Instant Client 경로 설정
# =============================
oracledb.init_oracle_client(lib_dir=r"C:\OCI\oracle_instant_client")

# =============================
# 2️⃣ Oracle 접속 정보
# =============================
username = 'DB_DETECTOR'
password = 'DB_DETECTOR'
dsn = 'localhost:1521/XE'

# =============================
# 3️⃣ SQL 파일 경로
# =============================
sql_file = './datasets/sql/insert_db_metrics_batch_2025_09_17_test.sql'

# =============================
# 4️⃣ SQL 파일 읽기
# =============================
with open(sql_file, 'r') as f:
    # 공백 제거 + 빈 줄 제거
    sql_commands = [cmd.strip() for cmd in f.read().split(';') if cmd.strip()]

TOTAL_BATCHES = len(sql_commands)
ROWS_PER_BATCH = 1000  # INSERT ALL 한 묶음당 row 수
TOTAL_ROWS = TOTAL_BATCHES * ROWS_PER_BATCH

print(f"총 {TOTAL_BATCHES} batch SQL 명령 확인됨. 총 row 수: 약 {TOTAL_ROWS}")

# =============================
# 5️⃣ DB 연결
# =============================
conn = oracledb.connect(user=username, password=password, dsn=dsn)
cursor = conn.cursor()

# =============================
# 6️⃣ 배치 단위로 insert + 커밋 + 진행률 출력
# =============================
batch_count = 0
inserted_rows = 0

for command in sql_commands:
    try:
        cursor.execute(command)
        batch_count += 1
        inserted_rows += ROWS_PER_BATCH

        # 한 command 단위로 커밋
        conn.commit()
        percent = round(inserted_rows / TOTAL_ROWS * 100, 2)
        print(f"✅ {inserted_rows}/{TOTAL_ROWS} rows inserted ({percent}% complete)")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        print(f"실패한 SQL 일부: {command[:100]}...")
        conn.rollback()
        break

# =============================
# 7️⃣ DB 종료
# =============================
conn.commit()
cursor.close()
conn.close()
print("✅ 모든 SQL 실행 완료! 데이터 삽입 완료")
