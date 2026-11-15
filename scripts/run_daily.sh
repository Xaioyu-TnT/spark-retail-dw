#!/bin/bash
# 每日增量调度脚本：由 crontab 调用
# 用法: run_daily.sh [日期]   不传日期则处理数据里的最大日期
set -u
LOG_DIR="$HOME/bigdata/logs"
mkdir -p "$LOG_DIR"
DAY=$(date +%Y%m%d)
LOG="$LOG_DIR/etl_$DAY.log"

cd "$HOME/bigdata" || exit 1
source "$HOME/sparkenv/bin/activate"

{
  echo "=============================================="
  echo "[$(date '+%F %T')] 开始执行增量 ETL"
  if [ $# -ge 1 ]; then
    python jobs/etl_incr.py "$1"
  else
    python jobs/etl_incr.py
  fi
  rc=$?
  echo "[$(date '+%F %T')] 结束，返回码 $rc"
} >> "$LOG" 2>&1

# 只保留最近 14 天日志
find "$LOG_DIR" -name 'etl_*.log' -mtime +14 -delete 2>/dev/null
