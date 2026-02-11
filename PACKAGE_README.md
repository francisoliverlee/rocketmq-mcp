# RocketMQ MCP 打包脚本使用说明

## 概述

打包脚本 `package.sh` 用于将 RocketMQ MCP 项目的 Maven 构建结果和测试脚本打包为 zip 文件，方便分发和部署。

## 功能特性

- ✅ 自动执行 Maven 构建
- ✅ 收集所有必要的文件到统一目录
- ✅ 生成包含版本信息的 zip 包
- ✅ 自动创建 README 说明文档
- ✅ 支持环境检查和错误处理

## 打包内容

打包后的 zip 文件包含以下内容：

### 核心文件
- `rocketmq-mcp-server-0.0.1.jar` - 主程序 JAR 文件
- `build.sh` - Maven 构建脚本
- `pom.xml` - Maven 项目配置
- `LICENSE` - 许可证文件

### 测试文件
- `test/` - 测试脚本目录
  - `test_rocketmq_mcp_unittest.py` - Python 单元测试
  - `main.py` - HTTP 接口测试
  - `quick_testing.sh` - 快速测试脚本
  - `requirements.txt` - Python 依赖
  - `run_unittest.sh` - 单元测试运行脚本

### 文档文件
- `README.md` - 项目说明和使用指南

## 使用方法

### 1. 执行打包脚本
```bash
# 在项目根目录执行
./package.sh
```

### 2. 查看打包结果
打包完成后，zip 文件将生成在 `dist/` 目录下：
```bash
ls dist/
# rocketmq-mcp-server-0.0.1.zip
```

### 3. 解压和使用
```bash
# 解压到当前目录
unzip dist/rocketmq-mcp-server-0.0.1.zip

# 进入解压后的目录
cd rocketmq-mcp-server-0.0.1

# 启动服务
java -jar rocketmq-mcp-server-0.0.1.jar

# 运行测试
cd test
./run_unittest.sh
```

## 环境要求

- **Maven**: 3.6+ (用于项目构建)
- **Java**: 17+ (Spring Boot 4.0.0 要求)
- **zip**: 用于打包工具
- **Python**: 3.6+ (用于测试脚本)

## 脚本执行流程

1. **环境检查**: 验证 Maven 和 zip 工具是否可用
2. **Maven 构建**: 执行 `mvn clean install` 构建项目
3. **文件收集**: 复制 JAR 文件、测试脚本和配置文件
4. **文档生成**: 创建 README.md 说明文档
5. **打包压缩**: 将所有文件打包为 zip 格式
6. **清理工作**: 删除临时文件

## 错误处理

- 如果 Maven 构建失败，脚本会立即停止并显示错误信息
- 如果缺少必要的工具，脚本会提示安装要求
- 所有操作都有详细的日志输出，便于排查问题

## 自定义配置

如需修改打包配置，可以编辑 `package.sh` 脚本中的变量：

```bash
# 项目配置
PROJECT_NAME="rocketmq-mcp-server"
VERSION="0.0.1"
PACKAGE_NAME="${PROJECT_NAME}-${VERSION}"
```

## 注意事项

- 打包前请确保所有代码变更已提交
- 测试脚本需要 Python 环境支持
- 打包过程会跳过单元测试以加快构建速度
- 生成的 zip 文件可用于生产环境部署