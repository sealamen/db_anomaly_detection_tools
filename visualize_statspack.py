import re
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc

# ================================
# 1️⃣ Statspack 텍스트 파일 읽기
# ================================
# file_path = "C:\\OCI\\repository\\db_anomaly_detection_tools\\statspack_20250922.txt"
file_path = 'statspack_20250922_1135.txt'


with open(file_path, "r", encoding="cp949") as f:
    text = f.read()

# ================================
# 2️⃣ Top 5 Timed Events
# ================================
def parse_top_events(text):
    pattern = r"Top 5 Timed Events.*?(?=^=|\Z)"
    match = re.search(pattern, text, flags=re.S | re.M)
    section = match.group(0) if match else ""
    rows = []
    for line in section.splitlines():
        line = line.strip()
        if not line or "Event" in line or "Waits" in line or "Time" in line:
            continue
        parts = re.split(r"\s{2,}", line)
        if len(parts) >= 3:
            event = parts[0]
            waits = parts[1].replace(",", "")
            time_s = parts[2].replace(",", "")
            try: waits_val = int(waits) if waits.isdigit() else 0
            except: waits_val = 0
            try: time_val = float(time_s)
            except: time_val = 0.0
            rows.append([event, waits_val, time_val])
    return pd.DataFrame(rows, columns=["Event", "Waits", "Time(s)"])

df_events = parse_top_events(text)

# ================================
# 3️⃣ Load Profile
# ================================
def parse_load_profile(text):
    pattern = r"Load Profile.*?(?=^=|\Z)"
    match = re.search(pattern, text, flags=re.S | re.M)
    section = match.group(0) if match else ""
    rows = []
    for line in section.splitlines():
        line = line.strip()
        if not line or "Per Second" in line or "Per Transaction" in line:
            continue
        parts = re.split(r"\s{2,}", line)
        if len(parts) >= 3:
            metric = parts[0]
            per_sec = parts[1].replace(",", "")
            per_txn = parts[2].replace(",", "")
            try: per_sec_val = float(per_sec)
            except: per_sec_val = 0.0
            try: per_txn_val = float(per_txn)
            except: per_txn_val = 0.0
            rows.append([metric, per_sec_val, per_txn_val])
    return pd.DataFrame(rows, columns=["Metric", "Per Second", "Per Transaction"])

df_load = parse_load_profile(text)

# ================================
# 4️⃣ Instance Efficiency
# ================================
def parse_instance_efficiency(text):
    pattern = r"Instance Efficiency Percent.*?(?=^=|\Z)"
    match = re.search(pattern, text, flags=re.S | re.M)
    section = match.group(0) if match else ""
    rows = []
    for line in section.splitlines():
        line = line.strip()
        if not line or "%" not in line:
            continue
        parts = re.split(r"\s{2,}", line)
        if len(parts) >= 2:
            metric = parts[0]
            try: value = float(parts[1].replace("%",""))
            except: value = 0.0
            rows.append([metric, value])
    return pd.DataFrame(rows, columns=["Metric", "Percent"])

df_efficiency = parse_instance_efficiency(text)

# ================================
# 5️⃣ Buffer Cache / Library Cache
# ================================
def parse_cache_stats(text, section_title, value_column="Percent"):
    pattern = rf"{section_title}.*?(?=^=|\Z)"
    match = re.search(pattern, text, flags=re.S | re.M)
    section = match.group(0) if match else ""
    rows = []
    for line in section.splitlines():
        line = line.strip()
        if not line or value_column in line:
            continue
        parts = re.split(r"\s{2,}", line)
        if len(parts) >= 2:
            metric = parts[0]
            try: value = float(parts[1].replace("%",""))
            except: value = 0.0
            rows.append([metric, value])
    return pd.DataFrame(rows, columns=["Metric", value_column])

df_buffer_cache = parse_cache_stats(text, "Buffer Cache Hit Ratio")
df_library_cache = parse_cache_stats(text, "Library Cache Hit Ratio")

# ================================
# 6️⃣ Redo Statistics
# ================================
def parse_redo_stats(text):
    pattern = r"Redo Statistics.*?(?=^=|\Z)"
    match = re.search(pattern, text, flags=re.S | re.M)
    section = match.group(0) if match else ""
    rows = []
    for line in section.splitlines():
        line = line.strip()
        if not line or "Redo" in line:
            continue
        parts = re.split(r"\s{2,}", line)
        if len(parts) >= 2:
            metric = parts[0]
            try: value = float(parts[1].replace(",", ""))
            except: value = 0.0
            rows.append([metric, value])
    return pd.DataFrame(rows, columns=["Metric", "Value"])

df_redo = parse_redo_stats(text)

# ================================
# 7️⃣ Dash 앱 생성
# ================================
app = Dash(__name__)
app.title = "Statspack Full Dashboard v2"

app.layout = html.Div([
    html.H1("Oracle 11g XE Statspack Full Dashboard v2", style={"textAlign": "center"}),

    html.H2("Top 5 Timed Events"),
    dcc.Graph(figure=px.bar(df_events, x="Event", y="Time(s)",
                            color="Time(s)", text="Time(s)", color_continuous_scale="Blues").update_traces(textposition="outside")),

    html.H2("Load Profile (Per Second)"),
    dcc.Graph(figure=px.bar(df_load, x="Metric", y="Per Second",
                            color="Per Second", text="Per Second", color_continuous_scale="Oranges").update_traces(textposition="outside")),

    html.H2("Load Profile (Per Transaction)"),
    dcc.Graph(figure=px.bar(df_load, x="Metric", y="Per Transaction",
                            color="Per Transaction", text="Per Transaction", color_continuous_scale="Greens").update_traces(textposition="outside")),

    html.H2("Instance Efficiency"),
    dcc.Graph(figure=px.bar(df_efficiency, x="Metric", y="Percent",
                            color="Percent", text="Percent", color_continuous_scale="Purples").update_traces(textposition="outside")),

    html.H2("Buffer Cache Hit Ratio"),
    dcc.Graph(figure=px.bar(df_buffer_cache, x="Metric", y="Percent",
                            color="Percent", text="Percent", color_continuous_scale="Teal").update_traces(textposition="outside")),

    html.H2("Library Cache Hit Ratio"),
    dcc.Graph(figure=px.bar(df_library_cache, x="Metric", y="Percent",
                            color="Percent", text="Percent", color_continuous_scale="Pink").update_traces(textposition="outside")),

    html.H2("Redo Statistics"),
    dcc.Graph(figure=px.bar(df_redo, x="Metric", y="Value",
                            color="Value", text="Value", color_continuous_scale="Reds").update_traces(textposition="outside"))
])

# ================================
# 8️⃣ 서버 실행
# ================================
if __name__ == "__main__":
    app.run(debug=True)



