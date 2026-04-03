import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = r'C:\hadoop\bin' + os.pathsep + os.environ.get('PATH', '')
import shutil
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sqlite3

# 设置 SQLite 数据库绝对路径，防止 Spark Executor 路径漂移
DB_PATH = os.path.abspath("metrics.db")
CHECKPOINT_DIR = os.path.abspath(os.path.join("checkpoints", "spark_processor"))
RESET_METRICS_ON_START = True
STARTING_OFFSETS = "latest"


def build_kafka_package_coordinate():
    """根据当前 PySpark 版本自动生成兼容的 Kafka 连接器坐标。"""
    spark_ver = pyspark.__version__.split("+")[0]
    major = int(spark_ver.split(".")[0])
    # Spark 4.x 使用 Scala 2.13；Spark 3.x 常用 Scala 2.12
    scala_binary = "2.13" if major >= 4 else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala_binary}:{spark_ver}"

def init_db():
    """初始化 SQLite 数据库表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metrics (
            window_start TEXT,
            window_end TEXT,
            count INTEGER
        )
    ''')
    if RESET_METRICS_ON_START:
        cursor.execute('DELETE FROM metrics')
    conn.commit()
    conn.close()

def save_to_sqlite(df, epoch_id):
    """
    foreachBatch 的自定义 Sink
    将每个微批次的聚合结果提取出来，并使用 SQLite 做到跨进程安全的并发写入。
    这也彻底避免了直接用 Spark append CSV 导致的“碎片文件”或“文件锁”问题。
    """
    # 不再依赖 toPandas，避免 PySpark 4.x 对 pandas>=2.2.0 的硬要求
    rows = df.select("window", "count").collect()
    if rows:
        insert_rows = []
        for r in rows:
            w = r["window"]
            insert_rows.append((
                w["start"].strftime('%Y-%m-%d %H:%M:%S'),
                w["end"].strftime('%Y-%m-%d %H:%M:%S'),
                int(r["count"]),
            ))

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT INTO metrics (window_start, window_end, count) VALUES (?, ?, ?)",
            insert_rows,
        )
        conn.commit()
        conn.close()

        print(f"[-] 批次 {epoch_id} 处理完毕 | 发现 {len(insert_rows)} 个滑动窗口状态。已写入 DB！")

def main():
    # 提前建表
    init_db()
    if RESET_METRICS_ON_START and os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)

    # 1. 初始化 SparkSession
    kafka_pkg = build_kafka_package_coordinate()
    print(f"检测到 PySpark 版本: {pyspark.__version__} | 使用 Kafka 连接器: {kafka_pkg}")

    spark = SparkSession.builder \
        .appName("RealTimeAnomalyDetection") \
        .master("local[*]") \
        .config("spark.jars.packages", kafka_pkg) \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.default.parallelism", "16") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession 初始化完成，正在连接 Kafka...")

    # 2. 定义 Kafka 中 JSON 消息的 Schema
    schema = StructType([
        StructField("ip", StringType(), True),
        StructField("timestamp_iso", StringType(), True),
        StructField("original_time", StringType(), True),
        StructField("method", StringType(), True),
        StructField("url", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("delta_t", DoubleType(), True)
    ])

    # 3. 读取 Kafka 流
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "web-logs") \
        .option("startingOffsets", STARTING_OFFSETS) \
        .load()

    # == 核心考点体现区开始 ==

    # 解析 JSON 并转换真实事件时间
    parsed_stream = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    event_stream = parsed_stream.withColumn("timestamp", col("timestamp_iso").cast("timestamp"))

    # [考点 1]：Lazy Evaluation (惰性求值)
    # 这里我们哪怕写了极其复杂的正则过滤（去除图片等静态资源），在没有 start() 之前也绝对不会真正消耗 CPU 读取数据。
    filtered_stream = event_stream.filter(col("url").isNotNull()) \
                                  .filter(~col("url").like("%.png")) \
                                  .filter(~col("url").like("%.jpg"))

    # [考点 3]：Memory-based 计算 (跨微批次状态存储) 与 [考点 2]：DAG/Shuffle
    # 设置 10秒滑动窗口，每 2秒滑动一次，带有 5秒水位线(Watermark) 容忍网络延迟。
    # 这种配置迫使 Spark 必须在内存(State Store)中维护上一个批次尚未过期的数据窗口。
    # 同时 groupBy 必定触发宽依赖（Shuffle），完美分割 Stage。
    windowed_counts = filtered_stream \
        .withWatermark("timestamp", "5 seconds") \
        .groupBy(window(col("timestamp"), "10 seconds", "2 seconds")) \
        .count()

    # == 核心考点体现区结束 ==

    print("计算逻辑 DAG 规划完毕，准备触发流式执行...")

    # 4. 触发 Action，启动真正的流式计算
    query = windowed_counts.writeStream \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="2 seconds") \
        .foreachBatch(save_to_sqlite) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
