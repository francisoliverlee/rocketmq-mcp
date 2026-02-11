#!/usr/bin/env python3
"""
Consumer工具单元测试
测试Consumer相关的HTTP接口功能
"""

import unittest
import requests
import json
import time
import os
from datetime import datetime
import sys


class TestConsumerTool(unittest.TestCase):
    """Consumer工具单元测试类"""
    
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
        cls.test_topic = os.getenv('TEST_TOPIC', 'test_topic')
        cls.consumer_group = os.getenv('TEST_CONSUMER_GROUP', 'test_consumer_group')
        cls.client_id = os.getenv('TEST_CLIENT_ID', 'test_client_id')
        
        cls.session = requests.Session()
        
        print(f"Testing Consumer Tool at: {cls.base_url}")
        print(f"NameServer: {cls.nameserver_address}")
        print(f"Consumer Group: {cls.consumer_group}")
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
        url = f"{cls.base_url}{endpoint}"
        
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
    
    def test_examine_consumer_connection_info(self):
        """测试检查消费者连接信息接口"""
        print("Testing examineConsumerConnectionInfo...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "consumerGroup": self.consumer_group
        }
        
        success, result = self.make_request("/consumer/examineConsumerConnectionInfo", data)
        
        if success:
            print("✓ examineConsumerConnectionInfo - Success")
        else:
            print(f"✗ examineConsumerConnectionInfo - {result}")
        
        self.assertTrue(success, f"examineConsumerConnectionInfo failed: {result}")
    
    def test_get_consumer_connection_list(self):
        """测试获取消费者连接列表接口"""
        print("Testing getConsumerConnectionList...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "consumerGroup": self.consumer_group
        }
        
        success, result = self.make_request("/consumer/getConsumerConnectionList", data)
        
        if success:
            print("✓ getConsumerConnectionList - Success")
        else:
            print(f"✗ getConsumerConnectionList - {result}")
        
        self.assertTrue(success, f"getConsumerConnectionList failed: {result}")
    
    def test_get_consumer_running_info(self):
        """测试获取消费者运行信息接口"""
        print("Testing getConsumerRunningInfo...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "consumerGroup": self.consumer_group,
            "clientId": self.client_id
        }
        
        success, result = self.make_request("/consumer/getConsumerRunningInfo", data)
        
        if success:
            print("✓ getConsumerRunningInfo - Success")
        else:
            print(f"✗ getConsumerRunningInfo - {result}")
        
        self.assertTrue(success, f"getConsumerRunningInfo failed: {result}")
    
    def test_get_consumer_status(self):
        """测试获取消费者状态接口"""
        print("Testing getConsumerStatus...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "consumerGroup": self.consumer_group
        }
        
        success, result = self.make_request("/consumer/getConsumerStatus", data)
        
        if success:
            print("✓ getConsumerStatus - Success")
        else:
            print(f"✗ getConsumerStatus - {result}")
        
        self.assertTrue(success, f"getConsumerStatus failed: {result}")
    
    def test_reset_offset(self):
        """测试重置消费偏移量接口"""
        print("Testing resetOffset...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "consumerGroup": self.consumer_group,
            "topic": self.test_topic,
            "resetTimestamp": int(time.time() * 1000)
        }
        
        success, result = self.make_request("/consumer/resetOffset", data)
        
        if success:
            print("✓ resetOffset - Success")
        else:
            print(f"✗ resetOffset - {result}")
        
        self.assertTrue(success, f"resetOffset failed: {result}")
    
    def test_get_consumer_metrics(self):
        """测试获取消费者指标接口"""
        print("Testing getConsumerMetrics...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "consumerGroup": self.consumer_group
        }
        
        success, result = self.make_request("/consumer/getConsumerMetrics", data)
        
        if success:
            print("✓ getConsumerMetrics - Success")
        else:
            print(f"✗ getConsumerMetrics - {result}")
        
        self.assertTrue(success, f"getConsumerMetrics failed: {result}")
    
    def test_query_consumer_offset(self):
        """测试查询消费者偏移量接口"""
        print("Testing queryConsumerOffset...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "consumerGroup": self.consumer_group,
            "topic": self.test_topic,
            "queueId": 0
        }
        
        success, result = self.make_request("/consumer/queryConsumerOffset", data)
        
        if success:
            print("✓ queryConsumerOffset - Success")
        else:
            print(f"✗ queryConsumerOffset - {result}")
        
        self.assertTrue(success, f"queryConsumerOffset failed: {result}")
    
    def test_search_offset_by_timestamp(self):
        """测试根据时间戳搜索偏移量接口"""
        print("Testing searchOffsetByTimestamp...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "queueId": 0,
            "timestamp": int(time.time() * 1000)
        }
        
        success, result = self.make_request("/consumer/searchOffsetByTimestamp", data)
        
        if success:
            print("✓ searchOffsetByTimestamp - Success")
        else:
            print(f"✗ searchOffsetByTimestamp - {result}")
        
        self.assertTrue(success, f"searchOffsetByTimestamp failed: {result}")
    
    def test_get_max_offset(self):
        """测试获取最大偏移量接口"""
        print("Testing getMaxOffset...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "queueId": 0
        }
        
        success, result = self.make_request("/consumer/getMaxOffset", data)
        
        if success:
            print("✓ getMaxOffset - Success")
        else:
            print(f"✗ getMaxOffset - {result}")
        
        self.assertTrue(success, f"getMaxOffset failed: {result}")
    
    def test_get_min_offset(self):
        """测试获取最小偏移量接口"""
        print("Testing getMinOffset...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "queueId": 0
        }
        
        success, result = self.make_request("/consumer/getMinOffset", data)
        
        if success:
            print("✓ getMinOffset - Success")
        else:
            print(f"✗ getMinOffset - {result}")
        
        self.assertTrue(success, f"getMinOffset failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("Consumer Tool Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()