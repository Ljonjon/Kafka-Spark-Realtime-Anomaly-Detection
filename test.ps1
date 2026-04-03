Write-Host "Starting Real-Time Anomaly Detection Pipeline (Optimized Version)..." -ForegroundColor Green

# 1. 启动 生产者 (后台/新窗口)
Write-Host "1/3 Starting Kafka Producer..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "conda activate base; python producer.py"

# 等待 Spark 初始化 (防止生产者发送太快导致起步数据丢失)
Start-Sleep -Seconds 8

# 2. 启动 Spark 处理器 (后台/新窗口)
Write-Host "2/3 Starting Spark Processor..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "conda activate base; python spark_processor.py"

Start-Sleep -Seconds 3

# 3. 启动 Streamlit Dashboard (后台/新窗口)
Write-Host "3/3 Starting Streamlit Dashboard..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "conda activate base; python -m streamlit run dashboard.py --server.headless true --server.port 8501"

Write-Host "All services started in separate windows." -ForegroundColor Green
