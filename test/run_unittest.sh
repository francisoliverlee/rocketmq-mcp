#!/bin/bash

# RocketMQ MCP 单元测试运行脚本
# 支持按分类运行所有单元测试文件，支持环境变量配置

set -e

echo "=== RocketMQ MCP 单元测试运行器 ==="

# 检查并加载环境变量配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# 如果存在.env文件，尝试加载
if [ -f "$ENV_FILE" ]; then
    echo "检测到.env文件，加载环境变量配置..."
    while IFS='=' read -r key value; do
        # 跳过注释行和空行
        if [[ ! $key =~ ^#.*$ ]] && [[ -n $key ]]; then
            # 去除值中的引号（如果有）
            value=$(echo "$value" | sed "s/^['\"]//" | sed "s/['\"]$//")
            # 导出环境变量
            export "$key=$value"
        fi
    done < "$ENV_FILE"
    echo "✓ 环境变量配置已加载"
fi

# 设置默认服务器地址，优先使用环境变量
if [ -n "$MCP_SERVER_URL" ]; then
    SERVER_URL="$MCP_SERVER_URL"
    echo "使用环境变量配置的服务器地址: $SERVER_URL"
elif [ $# -ge 1 ]; then
    SERVER_URL="$1"
    echo "使用命令行参数指定的服务器地址: $SERVER_URL"
else
    SERVER_URL="http://localhost:6868"
    echo "使用默认服务器地址: $SERVER_URL"
fi

echo ""

# 显示当前配置信息
echo "当前配置信息:"
if [ -n "$NAMESERVER_ADDR" ]; then
    echo "  NameServer地址: $NAMESERVER_ADDR"
else
    echo "  NameServer地址: 使用默认值"
fi

if [ -n "$MCP_AK" ]; then
    echo "  Access Key: $MCP_AK"
else
    echo "  Access Key: 使用默认值"
fi

if [ -n "$MCP_SK" ]; then
    echo "  Secret Key: [已设置]"
else
    echo "  Secret Key: 使用默认值"
fi

echo ""

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到python3，请安装Python 3.6或更高版本"
    exit 1
fi

# 检查Python版本
PYTHON_VERSION=$(python3 -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")
PYTHON_MAJOR=$(python3 -c "import sys; print(sys.version_info[0])")
PYTHON_MINOR=$(python3 -c "import sys; print(sys.version_info[1])")

echo "Python版本: $PYTHON_VERSION"

if [ $PYTHON_MAJOR -lt 3 ] || ([ $PYTHON_MAJOR -eq 3 ] && [ $PYTHON_MINOR -lt 6 ]); then
    echo "错误: 需要Python 3.6或更高版本，当前版本为$PYTHON_VERSION"
    exit 1
fi

# 检查unittest框架可用性
if ! python3 -c "import unittest" &> /dev/null; then
    echo "错误: unittest框架不可用，请检查Python安装"
    exit 1
fi

# 检查第三方依赖
echo "检查Python依赖包..."
if ! python3 -c "import requests" &> /dev/null; then
    echo "安装requests包..."
    pip3 install requests
fi

if ! python3 -c "import requests" &> /dev/null; then
    echo "错误: requests包安装失败，请手动安装"
    exit 1
fi

echo "✓ 所有依赖包检查通过"
echo ""

# 显示使用说明
show_usage() {
    echo "使用方法:"
    echo "  ./run_unittest.sh [服务器地址] [分类名称]"
    echo ""
    echo "参数说明:"
    echo "  服务器地址: 可选，默认为 http://localhost:6868"
    echo "  分类名称: 可选，指定运行特定分类的测试"
    echo ""
    echo "环境变量配置:"
    echo "  可以创建.env文件来自定义配置:"
    echo "  cp .env.example .env"
    echo "  # 编辑.env文件，设置您的实际配置"
    echo ""
    echo "可用分类:"
    echo "  all        - 运行所有分类的测试（默认）"
    echo "  controller - Controller工具测试"
    echo "  broker     - Broker工具测试"
    echo "  consumer   - Consumer工具测试"
    echo "  message    - Message工具测试"
    echo "  nameserver - Nameserver工具测试"
    echo "  topic      - Topic工具测试"
    echo "  acl        - ACL工具测试"
    echo "  cluster    - Cluster工具测试"
    echo "  producer   - Producer工具测试"
    echo "  consumequeue - ConsumeQueue工具测试"
    echo ""
    echo "示例:"
    echo "  ./run_unittest.sh                    # 运行所有测试"
    echo "  ./run_unittest.sh http://192.168.1.100:6868  # 指定服务器运行所有测试"
    echo "  ./run_unittest.sh http://localhost:6868 broker  # 只运行Broker测试"
    echo "  source load_env.sh && ./run_unittest.sh  # 使用环境变量配置运行测试"
}

# 处理命令行参数
CATEGORY="all"
if [ $# -ge 2 ]; then
    CATEGORY="$2"
fi

# 验证分类参数
VALID_CATEGORIES=("all" "controller" "broker" "consumer" "message" "nameserver" "topic" "acl" "cluster" "producer" "consumequeue")

if [[ ! " ${VALID_CATEGORIES[@]} " =~ " ${CATEGORY} " ]]; then
    echo "错误: 无效的分类 '$CATEGORY'"
    echo ""
    show_usage
    exit 1
fi

# 运行测试
echo "开始运行单元测试..."
echo "测试分类: $CATEGORY"
echo ""

if [ "$CATEGORY" = "all" ]; then
    # 运行所有测试
    python3 run_all_tests.py --url "$SERVER_URL"
else
    # 运行特定分类的测试
    python3 run_all_tests.py --url "$SERVER_URL" --category "$CATEGORY"
fi

echo ""
echo "单元测试执行完成！"
echo ""

# 显示测试文件统计
echo "=== 测试文件统计 ==="
TEST_FILES=$(ls test_*.py 2>/dev/null | wc -l)
echo "测试文件数量: $TEST_FILES"

if [ $TEST_FILES -gt 0 ]; then
    echo "测试文件列表:"
    ls test_*.py | sed 's/^/  /'
fi

echo ""
echo "提示: 可以使用以下命令查看详细帮助:"
echo "  python3 run_all_tests.py --help"
echo "  python3 run_all_tests.py --list"
echo ""
echo "环境变量配置帮助:"
echo "  cat .env.example  # 查看环境变量配置示例"
echo "  source load_env.sh # 加载环境变量配置"