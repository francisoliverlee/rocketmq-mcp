#!/bin/bash

# RocketMQ MCP unittest测试脚本
# 用法: ./quick_testing.sh [服务器地址]

set -e

echo "=== RocketMQ MCP unittest测试 ==="

# 设置默认服务器地址
SERVER_URL="http://localhost:6868"
if [ $# -ge 1 ]; then
    SERVER_URL="$1"
fi

echo "服务器地址: $SERVER_URL"
echo ""

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到python3，请安装Python 3.6或更高版本"
    exit 1
fi

# 检查Python版本（unittest需要Python 3.2+）
PYTHON_VERSION=$(python3 -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")
PYTHON_MAJOR=$(python3 -c "import sys; print(sys.version_info[0])")
PYTHON_MINOR=$(python3 -c "import sys; print(sys.version_info[1])")

echo "Python版本: $PYTHON_VERSION"

if [ $PYTHON_MAJOR -lt 3 ] || ([ $PYTHON_MAJOR -eq 3 ] && [ $PYTHON_MINOR -lt 2 ]); then
    echo "错误: unittest框架需要Python 3.2或更高版本，当前版本为$PYTHON_VERSION"
    exit 1
fi

# 检查unittest框架可用性
if ! python3 -c "import unittest" &> /dev/null; then
    echo "错误: unittest框架不可用，请检查Python安装"
    exit 1
fi

# 检查第三方依赖
if ! python3 -c "import requests" &> /dev/null; then
    echo "安装第三方依赖包..."
    pip3 install -r requirements.txt
fi

# 运行unittest测试
echo "开始运行unittest测试..."
python3 main.py

echo ""
echo "unittest测试完成！"