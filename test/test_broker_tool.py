#!/usr/bin/env python3
"""
Broker工具单元测试
测试Broker相关的HTTP接口功能
"""

import unittest
import requests
import json
import time
import os
from datetime import datetime
import sys


class TestBrokerTool(unittest.TestCase):
    """Broker工具单元测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 设置基础URL
        cls.base_url = os.getenv('MCP_SERVER_URL', 'http://localhost:6868')
        if len(sys.argv) > 1:
            cls.base_url = sys.argv[1]
        
        # 从环境变量获取配置
        cls.nameserver_address = os.getenv('NAMESERVER_ADDR', '127.0.0.1:9876')
        cls.ak = os.getenv('MCP_AK', 'test')
        cls.sk = os.getenv('MCP_SK', 'test')
        cls.broker_addr = os.getenv('BROKER_ADDR', '127.0.0.1:10911')
        cls.test_topic = os.getenv('TEST_TOPIC', 'test_topic')
        
        cls.session = requests.Session()
        
        print(f"Testing Broker Tool at: {cls.base_url}")
        print(f"NameServer: {cls.nameserver_address}")
        print(f"Broker: {cls.broker_addr}")
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
    
    def test_get_broker_runtime_stats(self):
        """测试获取Broker运行时统计接口"""
        print("Testing getBrokerRuntimeStats...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "brokerAddr": self.broker_addr
        }
        
        success, result = self.make_request("/broker/getBrokerRuntimeStats", data)
        
        if success:
            print("✓ getBrokerRuntimeStats - Success")
        else:
            print(f"✗ getBrokerRuntimeStats - {result}")
        
        self.assertTrue(success, f"getBrokerRuntimeStats failed: {result}")
    
    def test_get_broker_config(self):
        """测试获取Broker配置接口"""
        print("Testing getBrokerConfig...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "brokerAddr": self.broker_addr
        }
        
        success, result = self.make_request("/broker/getBrokerConfig", data)
        
        if success:
            print("✓ getBrokerConfig - Success")
        else:
            print(f"✗ getBrokerConfig - {result}")
        
        self.assertTrue(success, f"getBrokerConfig failed: {result}")
    
    def test_get_broker_cluster_info(self):
        """测试获取Broker集群信息接口"""
        print("Testing getBrokerClusterInfo...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk
        }
        
        success, result = self.make_request("/broker/getBrokerClusterInfo", data)
        
        if success:
            print("✓ getBrokerClusterInfo - Success")
        else:
            print(f"✗ getBrokerClusterInfo - {result}")
        
        self.assertTrue(success, f"getBrokerClusterInfo failed: {result}")
    
    def test_send_message_status(self):
        """测试发送消息状态接口"""
        print("Testing sendMessageStatus...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "brokerAddr": self.broker_addr,
            "topic": self.test_topic
        }
        
        success, result = self.make_request("/broker/sendMessageStatus", data)
        
        if success:
            print("✓ sendMessageStatus - Success")
        else:
            print(f"✗ sendMessageStatus - {result}")
        
        self.assertTrue(success, f"sendMessageStatus failed: {result}")
    
    def test_get_broker_stats_data(self):
        """测试获取Broker统计数据接口"""
        print("Testing getBrokerStatsData...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "brokerAddr": self.broker_addr,
            "statsName": "put_tps",
            "statsKey": self.test_topic
        }
        
        success, result = self.make_request("/broker/getBrokerStatsData", data)
        
        if success:
            print("✓ getBrokerStatsData - Success")
        else:
            print(f"✗ getBrokerStatsData - {result}")
        
        self.assertTrue(success, f"getBrokerStatsData failed: {result}")
    
    def test_clean_unused_topic(self):
        """测试清理未使用Topic接口"""
        print("Testing cleanUnusedTopic...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "brokerAddr": self.broker_addr
        }
        
        success, result = self.make_request("/broker/cleanUnusedTopic", data)
        
        if success:
            print("✓ cleanUnusedTopic - Success")
        else:
            print(f"✗ cleanUnusedTopic - {result}")
        
        self.assertTrue(success, f"cleanUnusedTopic failed: {result}")
    
    def test_clean_expired_consume_queue(self):
        """测试清理过期消费队列接口"""
        print("Testing cleanExpiredConsumeQueue...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "brokerAddr": self.broker_addr
        }
        
        success, result = self.make_request("/broker/cleanExpiredConsumeQueue", data)
        
        if success:
            print("✓ cleanExpiredConsumeQueue - Success")
        else:
            print(f"✗ cleanExpiredConsumeQueue - {result}")
        
        self.assertTrue(success, f"cleanExpiredConsumeQueue failed: {result}")
    
    def test_delete_topic_in_broker(self):
        """测试删除Broker中的Topic接口"""
        print("Testing deleteTopicInBroker...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "brokerAddr": self.broker_addr,
            "topic": self.test_topic
        }
        
        success, result = self.make_request("/broker/deleteTopicInBroker", data)
        
        if success:
            print("✓ deleteTopicInBroker - Success")
        else:
            print(f"✗ deleteTopicInBroker - {result}")
        
        self.assertTrue(success, f"deleteTopicInBroker failed: {result}")
    
    def test_get_broker_metrics(self):
        """测试获取Broker指标接口"""
        print("Testing getBrokerMetrics...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "brokerAddr": self.broker_addr
        }
        
        success, result = self.make_request("/broker/getBrokerMetrics", data)
        
        if success:
            print("✓ getBrokerMetrics - Success")
        else:
            print(f"✗ getBrokerMetrics - {result}")
        
        self.assertTrue(success, f"getBrokerMetrics failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("Broker Tool Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()