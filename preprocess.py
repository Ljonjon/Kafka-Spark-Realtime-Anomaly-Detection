import re
import json
from datetime import datetime
import os

LOG_FILE = "access_10000.log"
OUTPUT_FILE = "data_preprocessed.jsonl"

# 样例日志行: 54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /filter/27... HTTP/1.1" 200 30577 "-" "Mozilla/..." "-"
# 正则提取 IP, 时间, Method, URL, Status
LOG_PATTERN = re.compile(
    r'(?P<ip>\d+\.\d+\.\d+\.\d+) - - '
    r'\[(?P<time>.*?)\] '
    r'"(?P<method>\w+) (?P<url>.*?) HTTP/1\.\d+" '
    r'(?P<status>\d+) '
    r'(?P<bytes>\d+)'
)

def parse_time(time_str):
    # 时间格式如: 22/Jan/2019:03:56:14 +0330
    # 忽略时区部分，提取年月日时分秒进行时间差计算
    dt_str = time_str.split(' ')[0]
    return datetime.strptime(dt_str, "%d/%b/%Y:%H:%M:%S")

def preprocess_logs():
    if not os.path.exists(LOG_FILE):
        print(f"Error: {LOG_FILE} not found!")
        return

    print("开始预处理日志数据...")
    
    last_time = None
    processed_count = 0
    valid_count = 0
    
    with open(LOG_FILE, 'r', encoding='utf-8') as infile, \
         open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
         
        for line in infile:
            processed_count += 1
            match = LOG_PATTERN.search(line)
            if not match:
                continue
                
            data = match.groupdict()
            
            # 解析时间
            try:
                current_time = parse_time(data['time'])
            except Exception as e:
                continue
                
            # 计算 delta_t (与上一条日志的时间差，单位：秒)
            if last_time is None:
                delta_t = 0.0
            else:
                delta_t = (current_time - last_time).total_seconds()
                
            # 为防止数据中有乱序导致负数时间差，做个兜底
            delta_t = max(0.0, delta_t)
            
            last_time = current_time
            
            # 组成 JSON 结构，timestamp 保留原始字符串，便于 Spark 以后自己转换或者直接用我们转好的 ISO 格式
            json_record = {
                "ip": data['ip'],
                "timestamp_iso": current_time.isoformat(),
                "original_time": data['time'],
                "method": data['method'],
                "url": data['url'],
                "status": int(data['status']),
                "delta_t": delta_t
            }
            
            outfile.write(json.dumps(json_record) + '\n')
            valid_count += 1
            
            if valid_count % 2000 == 0:
                print(f"已处理 {valid_count} 条有效日志...")

    print(f"预处理完成！总行数: {processed_count}, 成功提取: {valid_count}")
    print(f"数据已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    preprocess_logs()
