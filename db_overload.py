import oracledb
import threading
import time
import random
from collections import defaultdict

# ====== 환경 설정 ======
INSTANT_CLIENT = r"C:\OCI\oracle_instant_client"    # 본인 환경에 맞게 수정(oci.dll 위치)
USER = "system"
PWD  = "1234"
DSN  = "192.168.4.208:1521/xe"

THREADS = 20
TARGET_QPS = 1000

# 액션 비율: extreme_spike 포함, 절반 이상 spike
ACTION_WEIGHTS = {
    "select_pk":         0.05,
    "select_range_full": 0.10,
    "insert_commit":     0.10,
    "update_some":       0.10,
    "parse_heavy":       0.10,
    "insert_rollback":   0.00,
    "extreme_spike":     0.55,  # spike 비율 대폭 증가
}

LOG_INTERVAL = 5

# ====== Thick 모드 초기화 ======
oracledb.init_oracle_client(lib_dir=INSTANT_CLIENT)

# ====== 모니터링용 전역 카운터 ======
counter_lock = threading.Lock()
counters = defaultdict(int)
start_time = time.time()

def pick_action():
    r = random.random()
    cum = 0.0
    for k, w in ACTION_WEIGHTS.items():
        cum += w
        if r <= cum:
            return k
    return "select_pk"

def extreme_spike(cur, conn, duration=10):
    """극단 부하: 10초 동안 반복적으로 INSERT+UPDATE+FULL SCAN"""
    end_time = time.perf_counter() + duration
    while time.perf_counter() < end_time:
        try:
            # INSERT
            payload = "X" * 1000
            cur.execute("INSERT INTO trans_test (id, payload) VALUES (trans_seq.NEXTVAL, :1)", [payload])
            # UPDATE
            id_val = random.randint(1, 5000)
            cur.execute("UPDATE big_table SET payload = :1 WHERE id = :2", ["Y"*200, id_val])
            # FULL SCAN
            cat_lo = random.randint(0, 900)
            cat_hi = cat_lo + random.randint(10, 100)
            cur.execute(f"SELECT /*+ FULL(b) */ COUNT(*) FROM big_table b WHERE b.cat BETWEEN {cat_lo} AND {cat_hi}")
            conn.commit()  # spike 동안 커밋 한 번
        except Exception as e:
            print(f"[Spike] Error: {e}")

def worker(thread_id, stop_event):
    conn = oracledb.connect(user=USER, password=PWD, dsn=DSN)
    cur = conn.cursor()
    per_thread_qps = TARGET_QPS / THREADS
    last = time.perf_counter()
    executed = 0

    try:
        while not stop_event.is_set():
            action = pick_action()

            if action == "select_pk":
                id_val = random.randint(1, 20000)
                cur.execute("SELECT payload FROM big_table WHERE id = :1", [id_val])

            elif action == "select_range_full":
                cat_lo = random.randint(0, 900)
                cat_hi = cat_lo + random.randint(5, 50)
                cur.execute(f"SELECT /*+ FULL(b) */ COUNT(*) FROM big_table b WHERE b.cat BETWEEN {cat_lo} AND {cat_hi}")

            elif action == "insert_commit":
                payload = "X" * 500
                cur.execute("INSERT INTO trans_test (id, payload) VALUES (trans_seq.NEXTVAL, :1)", [payload])
                conn.commit()

            elif action == "update_some":
                id_val = random.randint(1, 1000)
                cur.execute("UPDATE big_table SET payload = :1 WHERE id = :2", ["Y"*100, id_val])
                if random.random() < 0.2:
                    conn.commit()

            elif action == "parse_heavy":
                k = random.randint(0, 999)
                cur.execute(f"SELECT COUNT(*) FROM big_table WHERE cat = {k}")

            elif action == "insert_rollback":
                cur.execute("INSERT INTO trans_test (id, payload) VALUES (trans_seq.NEXTVAL, 'TEMP')")
                conn.rollback()

            elif action == "extreme_spike":
                extreme_spike(cur, conn, duration=10)

            with counter_lock:
                counters[action] += 1
                counters["total"] += 1

            executed += 1
            now = time.perf_counter()
            elapsed = now - last
            target_elapsed = executed / per_thread_qps
            if target_elapsed > elapsed:
                time.sleep(min(target_elapsed - elapsed, 0.02))

    except Exception as e:
        with counter_lock:
            counters[f"error_t{thread_id}"] += 1
        print(f"[Thread-{thread_id}] Error: {e}")
    finally:
        cur.close()
        conn.close()
        print(f"[Thread-{thread_id}] Connection closed")

def logger(stop_event):
    last_total = 0
    last_time = time.time()
    while not stop_event.is_set():
        time.sleep(LOG_INTERVAL)
        with counter_lock:
            total = counters["total"]
            snapshot = dict(counters)
        now = time.time()
        dt = now - last_time
        dcount = total - last_total
        qps = dcount / dt if dt > 0 else 0.0
        last_total = total
        last_time = now

        parts = [f"{k}:{snapshot.get(k,0)}" for k in ["select_pk","select_range_full","insert_commit","update_some","parse_heavy","insert_rollback","extreme_spike"]]
        print(f"[{time.strftime('%H:%M:%S')}] total={total:,}  +{dcount:,} in {dt:.1f}s  ~QPS={qps:.0f}  |  " + "  ".join(parts))

if __name__ == "__main__":
    stop_event = threading.Event()
    threads = []

    t_log = threading.Thread(target=logger, args=(stop_event,), daemon=True)
    t_log.start()

    for i in range(THREADS):
        t = threading.Thread(target=worker, args=(i+1, stop_event), daemon=True)
        t.start()
        threads.append(t)

    print("✅ Workload started. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Stopping workload...")
        stop_event.set()
        for t in threads:
            t.join(timeout=2)
        print("✅ Stopped.")
