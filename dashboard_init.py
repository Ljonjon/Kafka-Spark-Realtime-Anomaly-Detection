import streamlit as st
import sqlite3
import pandas as pd
import time
import plotly.express as px
import os

# 页面配置
st.set_page_config(page_title="Real-Time Anomaly Detection", page_icon="🚨", layout="wide")

# SQLite 数据源路径 (与 Spark 输出保持一致)
DB_PATH = os.path.abspath("metrics.db")

# 设定异常报警阈值
THRESHOLD = 500

def fetch_data():
    """从 SQLite 中安全读取最近 60 个滑动窗口的流量聚合结果"""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame(columns=['window_end', 'count'])
    
    try:
        conn = sqlite3.connect(DB_PATH)
        # 按照时间窗去重并取最大值（防止重叠写），抓取最近 60 条用于大屏展示
        query = '''
            SELECT window_end, MAX(count) as count 
            FROM metrics 
            GROUP BY window_end 
            ORDER BY window_end DESC 
            LIMIT 60
        '''
        df = pd.read_sql_query(query, conn)
        conn.close()

        # 统一类型，避免图表把数值当字符串
        df['count'] = pd.to_numeric(df['count'], errors='coerce').fillna(0).astype(int)
        df['window_end'] = pd.to_datetime(df['window_end'], errors='coerce')
        df = df.dropna(subset=['window_end'])
        
        # 倒序排列，保证图表的 X 轴时间线从左到右递增
        df = df.sort_values('window_end')
        return df
    except sqlite3.OperationalError:
        # SQLite 瞬间被写锁时，容忍一次抓取失败
        return pd.DataFrame(columns=['window_end', 'count'])

st.title("🚨 实时 Web 流量内存计算监控大屏")
st.markdown("基于 `Spark Structured Streaming` 的 **Sliding Window (滑动窗口)** 和内存 **State Store** 机制实现的实时异常检测系统。")

st.markdown("---")

# 使用 Streamlit 的 empty 占位符，实现局部刷新，避免整个网页闪烁白屏
metrics_placeholder = st.empty()
chart_placeholder = st.empty()
alert_placeholder = st.empty()

# 无限循环心跳：每 1 秒读取一次数据库重绘前端
while True:
    df = fetch_data()
    
    if not df.empty:
        # 提取当前（最新）的窗口指标
        latest_time = df['window_end'].iloc[-1]
        latest_time_str = latest_time.strftime('%Y-%m-%d %H:%M:%S')
        latest_count = int(df['count'].iloc[-1])
        prev_count = int(df['count'].iloc[-2]) if len(df) > 1 else 0
        delta_val = latest_count - prev_count
        
        # 1. 渲染顶部指标卡卡片
        with metrics_placeholder.container():
            col1, col2, col3 = st.columns(3)
            col1.metric(label="最新流量时间窗口", value=latest_time_str)
            col2.metric(label="当前并发访问数 (Count)", value=latest_count, delta=delta_val)
            col3.metric(label="DDoS 警报阈值", value=f"> {THRESHOLD}")

        # 2. 渲染动态实时折线图 (引入 Plotly 实现绚丽的高级交互图)
        fig = px.line(
            df, 
            x='window_end', 
            y='count', 
            markers=True,
            title="实时并发流量分析趋势图 (过去 60 个触发批次)"
        )
        
        # 常态流量较低时动态缩放 Y 轴，避免阈值线把细微波动压扁看起来像 0
        max_count = int(df['count'].max()) if not df.empty else 0
        if max_count < max(20, THRESHOLD // 5):
            upper = max(10, max_count * 3)
            fig.update_yaxes(range=[0, upper])
            fig.add_annotation(
                xref="paper",
                yref="paper",
                x=0.99,
                y=0.98,
                text=f"报警线={THRESHOLD}（当前缩放未显示）",
                showarrow=False,
                font=dict(color="red"),
            )
        else:
            fig.add_hline(y=THRESHOLD, line_dash="dash", line_color="red", annotation_text=f"报警线 ({THRESHOLD})")
        
        # 高亮超出阈值的数据点 (动态标红)
        if latest_count > THRESHOLD:
            fig.update_traces(line_color="crimson")
            
        fig.update_layout(
            yaxis_title="访问量 (请求/窗口)",
            xaxis_title="系统时间 (Window End)",
            template="plotly_dark",
            margin=dict(l=20, r=20, t=40, b=20)
        )
        
        chart_placeholder.plotly_chart(fig, use_container_width=True)
        
        # 3. 渲染报警动态组件
        if latest_count > THRESHOLD:
            alert_placeholder.error(f"⚠️ **【主控室红色警报】** 检测到超高突增流量！最新访问量达 **{latest_count}**，远超安全阈值！请立即介入！")
        else:
            alert_placeholder.success("✅ 当前网络流量平缓，未检测到大范围恶意攻击现象。Spark 引擎正在内存中实时护航。")
            
    else:
        chart_placeholder.info("⏳ 监控雷达就绪。正在等待 Spark 引擎将流处理计算结果持久化至本地数据库 (SQLite)...")
        alert_placeholder.empty()
        
    # 每隔 1 秒请求底层数据库最新记录进行刷新
    time.sleep(1)
