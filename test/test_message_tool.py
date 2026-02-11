#!/usr/bin/env python3
"""
Message工具单元测试
测试Message相关的HTTP接口功能
"""

import unittest
import requests
import json
import time
import os
from datetime import datetime
import sys


class TestMessageTool(unittest.TestCase):
    """Message工具单元测试类"""
    
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
        cls.test_broker = os.getenv('TEST_BROKER', 'test_broker')
        
        cls.session = requests.Session()
        
        print(f"Testing Message Tool at: {cls.base_url}")
        print(f"NameServer: {cls.nameserver_address}")
        print(f"Topic: {cls.test_topic}")
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
    
    def test_view_message(self):
        """测试查看消息接口"""
        print("Testing viewMessage...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "msgId": "test_message_id"
        }
        
        success, result = self.make_request("/message/viewMessage", data)
        
        if success:
            print("✓ viewMessage - Success")
        else:
            print(f"✗ viewMessage - {result}")
        
        self.assertTrue(success, f"viewMessage failed: {result}")
    
    def test_query_message_by_key(self):
        """测试根据消息Key查询消息接口"""
        print("Testing queryMessageByKey...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "key": "test_message_key"
        }
        
        success, result = self.make_request("/message/queryMessageByKey", data)
        
        if success:
            print("✓ queryMessageByKey - Success")
        else:
            print(f"✗ queryMessageByKey - {result}")
        
        self.assertTrue(success, f"queryMessageByKey failed: {result}")
    
    def test_query_message_by_id(self):
        """测试根据消息ID查询消息接口"""
        print("Testing queryMessageById...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "msgId": "test_message_id"
        }
        
        success, result = self.make_request("/message/queryMessageById", data)
        
        if success:
            print("✓ queryMessageById - Success")
        else:
            print(f"✗ queryMessageById - {result}")
        
        self.assertTrue(success, f"queryMessageById failed: {result}")
    
    def test_query_message_by_unique_key(self):
        """测试根据唯一Key查询消息接口"""
        print("Testing queryMessageByUniqueKey...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "uniqueKey": "test_unique_key"
        }
        
        success, result = self.make_request("/message/queryMessageByUniqueKey", data)
        
        if success:
            print("✓ queryMessageByUniqueKey - Success")
        else:
            print(f"✗ queryMessageByUniqueKey - {result}")
        
        self.assertTrue(success, f"queryMessageByUniqueKey failed: {result}")
    
    def test_query_message_by_offset(self):
        """测试根据偏移量查询消息接口"""
        print("Testing queryMessageByOffset...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "brokerName": self.test_broker,
            "queueId": 0,
            "offset": 100
        }
        
        success, result = self.make_request("/message/queryMessageByOffset", data)
        
        if success:
            print("✓ queryMessageByOffset - Success")
        else:
            print(f"✗ queryMessageByOffset - {result}")
        
        self.assertTrue(success, f"queryMessageByOffset failed: {result}")
    
    def test_query_message_by_timestamp(self):
        """测试根据时间戳查询消息接口"""
        print("Testing queryMessageByTimestamp...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "brokerName": self.test_broker,
            "queueId": 0,
            "timestamp": int(time.time() * 1000)
        }
        
        success, result = self.make_request("/message/queryMessageByTimestamp", data)
        
        if success:
            print("✓ queryMessageByTimestamp - Success")
        else:
            print(f"✗ queryMessageByTimestamp - {result}")
        
        self.assertTrue(success, f"queryMessageByTimestamp failed: {result}")
    
    def test_get_message_metrics(self):
        """测试获取消息指标接口"""
        print("Testing getMessageMetrics...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic
        }
        
        success, result = self.make_request("/message/getMessageMetrics", data)
        
        if success:
            print("✓ getMessageMetrics - Success")
        else:
            print(f"✗ getMessageMetrics - {result}")
        
        self.assertTrue(success, f"getMessageMetrics failed: {result}")
    
    def test_get_message_statistics(self):
        """测试获取消息统计接口"""
        print("Testing getMessageStatistics...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic
        }
        
        success, result = self.make_request("/message/getMessageStatistics", data)
        
        if success:
            print("✓ getMessageStatistics - Success")
        else:
            print(f"✗ getMessageStatistics - {result}")
        
        self.assertTrue(success, f"getMessageStatistics failed: {result}")
    
    def test_get_message_trace(self):
        """测试获取消息轨迹接口"""
        print("Testing getMessageTrace...")
        
        data = {
            "nameserverAddressList": [self.nameserver_address],
            "ak": self.ak,
            "sk": self.sk,
            "topic": self.test_topic,
            "msgId": "test_message_id"
        }
        
        success, result = self.make_request("/message/getMessageTrace", data)
        
        if success:
            print("✓ getMessageTrace - Success")
        else:
            print(f"✗ getMessageTrace - {result}")
        
        self.assertTrue(success, f"getMessageTrace failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("Message Tool Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()