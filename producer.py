import json
import time
import threading
from datetime import datetime
from kafka import KafkaProducer

# 配置常量
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'web-logs'
DATA_FILE = 'data_preprocessed.jsonl'
SPEED_UP = 2.0  # 重放加速倍率（如果嫌原来的日志太慢，可以调大这个值，如 2.0 表示2倍速）
REPLAY_FOREVER = True
REPLAY_ROUND_PAUSE = 1.0
FLUSH_EVERY = 200
ATTACK_COUNT = 500

def create_producer():
    """初始化 Kafka Producer"""
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # 为了演示突发，可以稍微调大 batch_size 或者减少 linger_ms
        linger_ms=10 
    )
    print("Kafka connected successfully.")
    return producer

def injection_listener(producer):
    """
    副线程：键盘监听与异常流量注入引擎
    使用标准 input() 阻塞在子线程，不会影响主线程的回放节奏
    """
    while True:
        # 子线程阻塞等待用户按下回车
        try:
            input(f"\n[守护线程] >>> 随时按下 Enter 键瞬间倾泻 {ATTACK_COUNT} 条恶意访问攻击 (DDoS) <<<\n")
        except EOFError:
            print("[守护线程] 当前终端不可交互，已关闭按键注入监听。")
            return
        print("\n[!] 警告：正在注入恶意突增流量...")

        # 固定同一事件时间，确保本次注入更容易落入同一批窗口中，演示结果可复现
        attack_ts = datetime.now().isoformat()
        
        for _ in range(ATTACK_COUNT):
            # 制造特定的恶意数据
            bad_record = {
                "ip": "192.168.1.100",  # 攻击者 IP
                "timestamp_iso": attack_ts,
                "original_time": "Attack_Injection",
                "method": "POST",
                "url": "/api/login",  # 爆破登录接口
                "status": 401, 
                "delta_t": 0.0
            }
            producer.send(TOPIC_NAME, value=bad_record)
        
        producer.flush() # 强制立刻把这批攻击流量刷入 Kafka
        print(f"[!] {ATTACK_COUNT} 条恶意攻击日志已成功送达 Kafka！可以观察大屏报警。\n")

def replay_historical_data(producer):
    """
    主线程：忠实回放历史数据，按原节拍平移时间
    """
    try:
        print("开始从历史文件匀速读取并重放数据...")
        total_count = 0
        round_id = 0

        while True:
            round_id += 1
            round_count = 0
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    record = json.loads(line)

                    # 1. 节奏控制：休眠原始的时间差 / 加速倍率
                    sleep_time = record.get('delta_t', 0.0) / SPEED_UP
                    if sleep_time > 0:
                        time.sleep(sleep_time)

                    # 2. 时间平移：将历史时间更新为“物理世界的当前时间”
                    record['timestamp_iso'] = datetime.now().isoformat()

                    # 3. 发送至 Kafka (异步发送，不阻塞)
                    producer.send(TOPIC_NAME, value=record)

                    total_count += 1
                    round_count += 1

                    if total_count % FLUSH_EVERY == 0:
                        producer.flush()

                    if total_count % 100 == 0:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] 正常水流：累计已发送 {total_count} 条（第 {round_id} 轮已发送 {round_count} 条）...")

            producer.flush()
            print(f"[i] 第 {round_id} 轮历史日志回放完成，共发送 {round_count} 条。")

            if not REPLAY_FOREVER:
                break

            time.sleep(REPLAY_ROUND_PAUSE)

    except FileNotFoundError:
        print(f"Error: 找不到预处理文件 {DATA_FILE}。请先运行 preprocess.py")
    except KeyboardInterrupt:
        print("\n[i] 收到终止信号，正在关闭 Producer...")
    except Exception as e:
        print(f"重放出现异常: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    producer = create_producer()
    
    # 启动副线程监听按下 Enter 注入攻击（设为 Daemon 随主线程退出）
    injector_thread = threading.Thread(target=injection_listener, args=(producer,), daemon=True)
    injector_thread.start()
    
    # 主线程启动日常平顺的数据重放
    replay_historical_data(producer)
