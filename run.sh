#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v python3 >/dev/null 2>&1; then
  echo "[ERROR] 未检测到 python3，请先安装 Python 3.10+"
  exit 1
fi

if [ ! -d .venv ]; then
  echo "[INFO] 创建虚拟环境 .venv"
  python3 -m venv .venv
fi

source .venv/bin/activate
python app.py
