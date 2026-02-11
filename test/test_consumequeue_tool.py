#!/usr/bin/env python3
"""
ConsumeQueue工具单元测试
测试ConsumeQueue相关的HTTP接口功能
"""

import unittest
import requests
import json
import time
from datetime import datetime
import sys


class TestConsumeQueueTool(unittest.TestCase):
    """ConsumeQueue工具单元测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 设置基础URL
        cls.base_url = "http://localhost:6868"
        if len(sys.argv) > 1:
            cls.base_url = sys.argv[1]
        
        cls.session = requests.Session()
        
        print(f"Testing ConsumeQueue Tool at: {cls.base_url}")
        print("=" * 60)
    
    def setUp(self):
        """每个测试方法前的准备工作"""
        self.start_time = time.time()
    
    def tearDown(self):
        """每个测试方法后的清理工作"""
        response_time = time.time() - self.start_time
        if hasattr(self, '_testMethodName'):
            print(f"{self._testMethodName} - {response_time:.2f}s")
    
    def make_request(self, endpoint, data=None, method="POST"):
        """发送HTTP请求的辅助方法"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method == "POST":
                response = self.session.post(url, json=data, timeout=30)
            else:
                response = self.session.get(url, timeout=30)
                
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, f"HTTP {response.status_code}: {response.text}"
                
        except Exception as e:
            return False, str(e)
    
    def test_server_connectivity(self):
        """测试服务器连接性"""
        print("Testing server connectivity...")
        
        try:
            response = self.session.get(f"{self.base_url}/actuator/health", timeout=10)
            self.assertEqual(response.status_code, 200, 
                           f"Server returned HTTP {response.status_code}")
            print("✓ Server is accessible")
        except Exception as e:
            self.fail(f"Cannot connect to server: {e}")
    
    def test_examine_consume_queue(self):
        """测试检查消费队列接口"""
        print("Testing examineConsumeQueue...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911"
        }
        
        success, result = self.make_request("/consumequeue/examineConsumeQueue", data)
        
        if success:
            print("✓ examineConsumeQueue - Success")
        else:
            print(f"✗ examineConsumeQueue - {result}")
        
        self.assertTrue(success, f"examineConsumeQueue failed: {result}")
    
    def test_get_consume_queue_info(self):
        """测试获取消费队列信息接口"""
        print("Testing getConsumeQueueInfo...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911",
            "queueId": 0
        }
        
        success, result = self.make_request("/consumequeue/getConsumeQueueInfo", data)
        
        if success:
            print("✓ getConsumeQueueInfo - Success")
        else:
            print(f"✗ getConsumeQueueInfo - {result}")
        
        self.assertTrue(success, f"getConsumeQueueInfo failed: {result}")
    
    def test_get_consume_queue_metrics(self):
        """测试获取消费队列指标接口"""
        print("Testing getConsumeQueueMetrics...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911",
            "queueId": 0
        }
        
        success, result = self.make_request("/consumequeue/getConsumeQueueMetrics", data)
        
        if success:
            print("✓ getConsumeQueueMetrics - Success")
        else:
            print(f"✗ getConsumeQueueMetrics - {result}")
        
        self.assertTrue(success, f"getConsumeQueueMetrics failed: {result}")
    
    def test_get_consume_queue_statistics(self):
        """测试获取消费队列统计信息接口"""
        print("Testing getConsumeQueueStatistics...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911",
            "queueId": 0
        }
        
        success, result = self.make_request("/consumequeue/getConsumeQueueStatistics", data)
        
        if success:
            print("✓ getConsumeQueueStatistics - Success")
        else:
            print(f"✗ getConsumeQueueStatistics - {result}")
        
        self.assertTrue(success, f"getConsumeQueueStatistics failed: {result}")
    
    def test_get_consume_queue_config(self):
        """测试获取消费队列配置接口"""
        print("Testing getConsumeQueueConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911",
            "queueId": 0
        }
        
        success, result = self.make_request("/consumequeue/getConsumeQueueConfig", data)
        
        if success:
            print("✓ getConsumeQueueConfig - Success")
        else:
            print(f"✗ getConsumeQueueConfig - {result}")
        
        self.assertTrue(success, f"getConsumeQueueConfig failed: {result}")
    
    def test_update_consume_queue_config(self):
        """测试更新消费队列配置接口"""
        print("Testing updateConsumeQueueConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911",
            "queueId": 0,
            "config": {
                "consumeBatchSize": 32,
                "pullBatchSize": 32,
                "suspendTimeoutMillis": 1000
            }
        }
        
        success, result = self.make_request("/consumequeue/updateConsumeQueueConfig", data)
        
        if success:
            print("✓ updateConsumeQueueConfig - Success")
        else:
            print(f"✗ updateConsumeQueueConfig - {result}")
        
        self.assertTrue(success, f"updateConsumeQueueConfig failed: {result}")
    
    def test_get_consume_queue_health(self):
        """测试获取消费队列健康状态接口"""
        print("Testing getConsumeQueueHealth...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911",
            "queueId": 0
        }
        
        success, result = self.make_request("/consumequeue/getConsumeQueueHealth", data)
        
        if success:
            print("✓ getConsumeQueueHealth - Success")
        else:
            print(f"✗ getConsumeQueueHealth - {result}")
        
        self.assertTrue(success, f"getConsumeQueueHealth failed: {result}")
    
    def test_get_consume_queue_performance(self):
        """测试获取消费队列性能指标接口"""
        print("Testing getConsumeQueuePerformance...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911",
            "queueId": 0
        }
        
        success, result = self.make_request("/consumequeue/getConsumeQueuePerformance", data)
        
        if success:
            print("✓ getConsumeQueuePerformance - Success")
        else:
            print(f"✗ getConsumeQueuePerformance - {result}")
        
        self.assertTrue(success, f"getConsumeQueuePerformance failed: {result}")
    
    def test_reset_consume_queue_offset(self):
        """测试重置消费队列偏移量接口"""
        print("Testing resetConsumeQueueOffset...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911",
            "queueId": 0,
            "consumerGroup": "test_group"
        }
        
        success, result = self.make_request("/consumequeue/resetConsumeQueueOffset", data)
        
        if success:
            print("✓ resetConsumeQueueOffset - Success")
        else:
            print(f"✗ resetConsumeQueueOffset - {result}")
        
        self.assertTrue(success, f"resetConsumeQueueOffset failed: {result}")
    
    def test_get_consume_queue_messages(self):
        """测试获取消费队列消息接口"""
        print("Testing getConsumeQueueMessages...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911",
            "queueId": 0,
            "offset": 0,
            "maxMsgNums": 10
        }
        
        success, result = self.make_request("/consumequeue/getConsumeQueueMessages", data)
        
        if success:
            print("✓ getConsumeQueueMessages - Success")
        else:
            print(f"✗ getConsumeQueueMessages - {result}")
        
        self.assertTrue(success, f"getConsumeQueueMessages failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("ConsumeQueue Tool Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()