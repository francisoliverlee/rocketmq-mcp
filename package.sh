#!/bin/bash

# RocketMQ MCP 打包脚本
# 将Maven构建结果和测试脚本打包为zip包

set -e

echo "=== RocketMQ MCP 打包脚本 ==="

# 设置变量
PROJECT_NAME="rocketmq-mcp-server"
VERSION="0.0.1"
PACKAGE_NAME="${PROJECT_NAME}-${VERSION}"
BUILD_DIR="target"
TEST_DIR="test"
OUTPUT_DIR="dist"
ZIP_FILE="${PACKAGE_NAME}.zip"

echo "项目名称: $PROJECT_NAME"
echo "版本: $VERSION"
echo "打包文件: $ZIP_FILE"
echo ""

# 检查Maven环境
if ! command -v mvn &> /dev/null; then
    echo "错误: 未找到Maven，请安装Maven 3.6或更高版本"
    exit 1
fi

# 检查zip命令
if ! command -v zip &> /dev/null; then
    echo "错误: 未找到zip命令，请安装zip工具"
    exit 1
fi

# 清理并创建输出目录
echo "清理输出目录..."
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# 执行Maven构建
echo "执行Maven构建..."
mvn clean install -Dmaven.test.skip=true

if [ $? -ne 0 ]; then
    echo "错误: Maven构建失败"
    exit 1
fi

echo "Maven构建成功"
echo ""

# 创建临时打包目录
echo "创建临时打包目录..."
TEMP_DIR="$OUTPUT_DIR/$PACKAGE_NAME"
rm -rf "$TEMP_DIR"
mkdir -p "$TEMP_DIR"

# 复制Maven构建结果
echo "复制Maven构建结果..."
if [ -f "$BUILD_DIR/$PROJECT_NAME-$VERSION.jar" ]; then
    cp "$BUILD_DIR/$PROJECT_NAME-$VERSION.jar" "$TEMP_DIR/"
    echo "✓ 复制JAR文件: $PROJECT_NAME-$VERSION.jar"
else
    echo "警告: 未找到JAR文件，检查构建是否成功"
fi

# 复制启动脚本
echo "复制启动脚本..."
if [ -f "build.sh" ]; then
    cp "build.sh" "$TEMP_DIR/"
    echo "✓ 复制构建脚本: build.sh"
fi

# 复制测试相关文件
echo "复制测试相关文件..."
if [ -d "$TEST_DIR" ]; then
    mkdir -p "$TEMP_DIR/$TEST_DIR"
    
    # 复制测试脚本
    for file in "$TEST_DIR"/*.py "$TEST_DIR"/*.sh "$TEST_DIR"/*.txt; do
        if [ -f "$file" ]; then
            cp "$file" "$TEMP_DIR/$TEST_DIR/"
            echo "✓ 复制测试文件: $(basename "$file")"
        fi
    done
    
    # 排除缓存目录
    rm -rf "$TEMP_DIR/$TEST_DIR/__pycache__" 2>/dev/null || true
else
    echo "警告: 未找到test目录"
fi

# 复制项目配置文件
echo "复制项目配置文件..."
for file in "pom.xml" "LICENSE" ".gitignore"; do
    if [ -f "$file" ]; then
        cp "$file" "$TEMP_DIR/"
        echo "✓ 复制配置文件: $file"
    fi
done

# 创建README文件
echo "创建README文件..."
cat > "$TEMP_DIR/README.md" << EOF
# RocketMQ MCP Server $VERSION

## 项目说明
这是一个RocketMQ MCP服务器，提供了RocketMQ的管理和监控功能。

## 文件说明
- `rocketmq-mcp-server-$VERSION.jar`: 主程序JAR文件
- `build.sh`: 构建脚本
- `test/`: 测试脚本目录
- `pom.xml`: Maven项目配置

## 运行方式
1. 启动服务: `java -jar rocketmq-mcp-server-$VERSION.jar`
2. 运行测试: 进入test目录执行相应测试脚本

## 测试说明
测试脚本位于test目录下，包含:
- Python单元测试脚本
- HTTP接口测试脚本
- 快速测试脚本

EOF

echo "✓ 创建README.md"

# 打包为zip文件
echo "创建zip包..."
cd "$OUTPUT_DIR"
zip -r "$ZIP_FILE" "$PACKAGE_NAME" > /dev/null
cd ..

# 清理临时目录
echo "清理临时文件..."
rm -rf "$TEMP_DIR"

echo ""
echo "=== 打包完成 ==="
echo "打包文件: $OUTPUT_DIR/$ZIP_FILE"
echo "文件大小: $(du -h "$OUTPUT_DIR/$ZIP_FILE" | cut -f1)"
echo ""

# 显示打包内容
echo "打包内容:"
unzip -l "$OUTPUT_DIR/$ZIP_FILE" | head -20
echo "..."

echo "打包成功！"