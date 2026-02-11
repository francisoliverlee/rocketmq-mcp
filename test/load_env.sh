#!/bin/bash

# RocketMQ MCP 环境变量加载脚本
# 用于加载.env文件中的配置到当前Shell环境

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# 检查.env文件是否存在
if [ ! -f "$ENV_FILE" ]; then
    echo "错误: 未找到.env文件"
    echo "请先复制.env.example为.env并修改配置:"
    echo "  cp .env.example .env"
    echo "  # 编辑.env文件，设置您的实际配置"
    exit 1
fi

# 加载环境变量
echo "加载环境变量配置..."
while IFS='=' read -r key value; do
    # 跳过注释行和空行
    if [[ ! $key =~ ^#.*$ ]] && [[ -n $key ]]; then
        # 去除值中的引号（如果有）
        value=$(echo "$value" | sed "s/^['\"]//" | sed "s/['\"]$//")
        # 导出环境变量
        export "$key=$value"
        echo "  $key=$value"
    fi
done < "$ENV_FILE"

echo ""
echo "环境变量加载完成！"
echo ""

# 显示当前配置
echo "当前配置:"
echo "  MCP服务器: $MCP_SERVER_URL"
echo "  NameServer: $NAMESERVER_ADDR"
echo "  Broker地址: $BROKER_ADDR"
echo "  AK: $MCP_AK"
echo "  SK: $MCP_SK"
echo "  测试Topic: $TEST_TOPIC"
echo ""

# 提示运行测试
echo "现在可以运行单元测试:"
echo "  ./run_unittest.sh"
echo "或"
echo "  python3 run_all_tests.py"
echo ""

# 检查是否在交互式Shell中运行
if [[ $- == *i* ]]; then
    echo "环境变量已加载到当前Shell会话中"
    echo "您可以直接运行测试命令"
else
    echo "注意: 此脚本必须在当前Shell中运行以生效:"
    echo "  source load_env.sh"
    echo "或"
    echo "  . load_env.sh"
fi