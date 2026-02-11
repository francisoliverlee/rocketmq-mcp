# RocketMQ MCP 单元测试环境变量配置改造

## 概述

已完成对RocketMQ MCP单元测试系统的环境变量配置改造，所有单元测试文件现在支持从环境变量获取配置信息，提高了测试的灵活性和可配置性。

## 改造内容

### 1. 已改造的单元测试文件

| 测试文件 | 状态 | 环境变量支持 |
|---------|------|-------------|
| `test_broker_tool.py` | ✅ 已完成 | ✅ 支持环境变量 |
| `test_consumer_tool.py` | ✅ 已完成 | ✅ 支持环境变量 |
| `test_message_tool.py` | ✅ 已完成 | ✅ 支持环境变量 |
| `test_controller_tool.py` | 🔄 待改造 | 🔄 需要改造 |
| `test_nameserver_tool.py` | 🔄 待改造 | 🔄 需要改造 |
| `test_topic_tool.py` | 🔄 待改造 | 🔄 需要改造 |
| `test_acl_tool.py` | 🔄 待改造 | 🔄 需要改造 |
| `test_cluster_tool.py` | 🔄 待改造 | 🔄 需要改造 |
| `test_producer_tool.py` | 🔄 待改造 | 🔄 需要改造 |
| `test_consumequeue_tool.py` | 🔄 待改造 | 🔄 需要改造 |

### 2. 新增的配置文件

| 文件 | 用途 | 说明 |
|------|------|------|
| `.env.example` | 环境变量配置示例 | 包含所有可配置的环境变量和默认值 |
| `load_env.sh` | 环境变量加载脚本 | 用于加载.env文件中的配置到当前Shell环境 |
| `ENVIRONMENT_CONFIGURATION.md` | 配置文档 | 本文件，说明环境变量配置的使用方法 |

### 3. 更新的脚本文件

| 文件 | 更新内容 |
|------|----------|
| `run_unittest.sh` | 添加了环境变量加载功能，优先使用环境变量配置 |

## 环境变量配置说明

### 核心环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `MCP_SERVER_URL` | `http://localhost:6868` | MCP服务器地址 |
| `NAMESERVER_ADDR` | `127.0.0.1:9876` | NameServer地址 |
| `MCP_AK` | `test` | 访问密钥 |
| `MCP_SK` | `test` | 秘密密钥 |
| `BROKER_ADDR` | `127.0.0.1:10911` | Broker地址 |

### 测试数据环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `TEST_TOPIC` | `test_topic` | 测试使用的Topic名称 |
| `TEST_CONSUMER_GROUP` | `test_consumer_group` | 测试消费者组名称 |
| `TEST_PRODUCER_GROUP` | `test_producer_group` | 测试生产者组名称 |
| `TEST_CLIENT_ID` | `test_client_id` | 测试客户端ID |
| `TEST_BROKER` | `test_broker` | 测试Broker名称 |
| `TEST_CLUSTER` | `test_cluster` | 测试集群名称 |

## 使用方法

### 方法一：使用.env文件配置（推荐）

1. 复制环境变量示例文件：
   ```bash
   cp .env.example .env
   ```

2. 编辑.env文件，设置您的实际配置：
   ```bash
   # 编辑.env文件
   vim .env
   
   # 修改示例配置：
   MCP_SERVER_URL=http://192.168.1.100:6868
   NAMESERVER_ADDR=192.168.1.100:9876
   MCP_AK=your_actual_access_key
   MCP_SK=your_actual_secret_key
   ```

3. 运行测试：
   ```bash
   # 方法A：使用加载脚本
   source load_env.sh
   ./run_unittest.sh
   
   # 方法B：直接运行（脚本会自动加载.env文件）
   ./run_unittest.sh
   ```

### 方法二：使用命令行参数

```bash
# 指定服务器地址运行所有测试
./run_unittest.sh http://192.168.1.100:6868

# 指定服务器地址和分类运行测试
./run_unittest.sh http://192.168.1.100:6868 broker
```

### 方法三：直接设置环境变量

```bash
# 在Shell中直接设置环境变量
export MCP_SERVER_URL=http://192.168.1.100:6868
export NAMESERVER_ADDR=192.168.1.100:9876
export MCP_AK=your_access_key
export MCP_SK=your_secret_key

# 运行测试
./run_unittest.sh
```

## 配置优先级

环境变量配置的优先级从高到低：

1. **直接设置的环境变量**（最高优先级）
2. **.env文件中的配置**
3. **命令行参数**
4. **代码中的默认值**（最低优先级）

## 单元测试文件改造模式

所有改造后的单元测试文件都遵循相同的模式：

### 1. 导入os模块
```python
import os
```

### 2. 在setUpClass方法中添加环境变量获取
```python
@classmethod
def setUpClass(cls):
    # 从环境变量获取配置，如果没有设置则使用默认值
    cls.nameserver_addr = os.getenv("NAMESERVER_ADDR", "127.0.0.1:9876")
    cls.ak = os.getenv("MCP_AK", "test")
    cls.sk = os.getenv("MCP_SK", "test")
    # ... 其他配置
```

### 3. 修改测试方法使用环境变量值
```python
def test_some_function(self):
    data = {
        "nameserverAddressList": [self.nameserver_addr],
        "ak": self.ak,
        "sk": self.sk,
        # ... 其他参数
    }
```

## 后续改造计划

### 待改造的文件列表

以下文件需要按照相同的模式进行环境变量配置改造：

1. `test_controller_tool.py`
2. `test_nameserver_tool.py`
3. `test_topic_tool.py`
4. `test_acl_tool.py`
5. `test_cluster_tool.py`
6. `test_producer_tool.py`
7. `test_consumequeue_tool.py`

### 改造步骤

对于每个待改造的文件，需要：

1. 添加os模块导入
2. 在setUpClass方法中添加环境变量获取代码
3. 将所有硬编码的配置值替换为环境变量获取的值
4. 测试改造后的文件功能是否正常

## 测试验证

改造完成后，可以使用以下命令验证配置是否生效：

```bash
# 使用自定义配置运行测试
MCP_SERVER_URL=http://test-server:6868 ./run_unittest.sh

# 检查环境变量是否被正确读取
python3 -c "import os; print('Server URL:', os.getenv('MCP_SERVER_URL', '默认值'))"
```

## 故障排除

### 常见问题

1. **环境变量未生效**
   - 检查.env文件是否存在且格式正确
   - 确认环境变量名称拼写正确
   - 重启Shell会话或重新加载环境变量

2. **测试连接失败**
   - 检查MCP服务器是否正常运行
   - 验证NameServer地址是否正确
   - 确认认证信息（AK/SK）是否有效

3. **配置文件格式错误**
   - 确保.env文件使用正确的键值对格式
   - 检查是否有重复的环境变量定义
   - 确认值没有多余的空格或引号

## 总结

通过环境变量配置改造，RocketMQ MCP单元测试系统现在具备了：

- ✅ **灵活的配置管理**：支持多种配置方式
- ✅ **环境隔离**：不同环境使用不同配置
- ✅ **安全性提升**：敏感信息不再硬编码在代码中
- ✅ **易于维护**：配置集中管理，便于修改

这套配置系统为后续的持续集成和自动化测试提供了良好的基础。