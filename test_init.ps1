Write-Host "Starting Real-Time Anomaly Detection Pipeline (Init Version)..." -ForegroundColor Green

# 1. 启动 生产者 _init (后台/新窗口)
Write-Host "1/3 Starting Init Kafka Producer..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "conda activate base; python producer_init.py"

Start-Sleep -Seconds 8

# 2. 启动 Spark 处理器 _init (后台/新窗口)
Write-Host "2/3 Starting Init Spark Processor..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "conda activate base; python spark_processor_init.py"

Start-Sleep -Seconds 3

# 3. 启动 Streamlit Dashboard _init (后台/新窗口)
Write-Host "3/3 Starting Init Streamlit Dashboard..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "conda activate base; python -m streamlit run dashboard_init.py --server.headless true --server.port 8501"

Write-Host "All INIT services started in separate windows." -ForegroundColor Green
