#!/usr/bin/env python3
"""
Topic工具单元测试
测试Topic相关的HTTP接口功能
"""

import unittest
import requests
import json
import time
from datetime import datetime
import sys


class TestTopicTool(unittest.TestCase):
    """Topic工具单元测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 设置基础URL
        cls.base_url = "http://localhost:6868"
        if len(sys.argv) > 1:
            cls.base_url = sys.argv[1]
        
        cls.session = requests.Session()
        
        print(f"Testing Topic Tool at: {cls.base_url}")
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
    
    def test_topic_list(self):
        """测试获取Topic列表接口"""
        print("Testing topicList...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        
        success, result = self.make_request("/topic/topicList", data)
        
        if success:
            print("✓ topicList - Success")
        else:
            print(f"✗ topicList - {result}")
        
        self.assertTrue(success, f"topicList failed: {result}")
    
    def test_create_topic(self):
        """测试创建Topic接口"""
        print("Testing createTopic...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "defaultTopicQueueNums": 8,
            "perm": 6
        }
        
        success, result = self.make_request("/topic/createTopic", data)
        
        if success:
            print("✓ createTopic - Success")
        else:
            print(f"✗ createTopic - {result}")
        
        self.assertTrue(success, f"createTopic failed: {result}")
    
    def test_delete_topic(self):
        """测试删除Topic接口"""
        print("Testing deleteTopic...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic"
        }
        
        success, result = self.make_request("/topic/deleteTopic", data)
        
        if success:
            print("✓ deleteTopic - Success")
        else:
            print(f"✗ deleteTopic - {result}")
        
        self.assertTrue(success, f"deleteTopic failed: {result}")
    
    def test_delete_topic_in_broker(self):
        """测试删除Broker中的Topic接口"""
        print("Testing deleteTopicInBroker...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerName": "test_broker"
        }
        
        success, result = self.make_request("/topic/deleteTopicInBroker", data)
        
        if success:
            print("✓ deleteTopicInBroker - Success")
        else:
            print(f"✗ deleteTopicInBroker - {result}")
        
        self.assertTrue(success, f"deleteTopicInBroker failed: {result}")
    
    def test_examine_topic_route_info(self):
        """测试检查Topic路由信息接口"""
        print("Testing examineTopicRouteInfo...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic"
        }
        
        success, result = self.make_request("/topic/examineTopicRouteInfo", data)
        
        if success:
            print("✓ examineTopicRouteInfo - Success")
        else:
            print(f"✗ examineTopicRouteInfo - {result}")
        
        self.assertTrue(success, f"examineTopicRouteInfo failed: {result}")
    
    def test_examine_topic_config(self):
        """测试检查Topic配置接口"""
        print("Testing examineTopicConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic"
        }
        
        success, result = self.make_request("/topic/examineTopicConfig", data)
        
        if success:
            print("✓ examineTopicConfig - Success")
        else:
            print(f"✗ examineTopicConfig - {result}")
        
        self.assertTrue(success, f"examineTopicConfig failed: {result}")
    
    def test_examine_topic_stats(self):
        """测试检查Topic统计信息接口"""
        print("Testing examineTopicStats...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic"
        }
        
        success, result = self.make_request("/topic/examineTopicStats", data)
        
        if success:
            print("✓ examineTopicStats - Success")
        else:
            print(f"✗ examineTopicStats - {result}")
        
        self.assertTrue(success, f"examineTopicStats failed: {result}")
    
    def test_examine_topic_subscription(self):
        """测试检查Topic订阅信息接口"""
        print("Testing examineTopicSubscription...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic"
        }
        
        success, result = self.make_request("/topic/examineTopicSubscription", data)
        
        if success:
            print("✓ examineTopicSubscription - Success")
        else:
            print(f"✗ examineTopicSubscription - {result}")
        
        self.assertTrue(success, f"examineTopicSubscription failed: {result}")
    
    def test_get_topic_metrics(self):
        """测试获取Topic指标接口"""
        print("Testing getTopicMetrics...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic"
        }
        
        success, result = self.make_request("/topic/getTopicMetrics", data)
        
        if success:
            print("✓ getTopicMetrics - Success")
        else:
            print(f"✗ getTopicMetrics - {result}")
        
        self.assertTrue(success, f"getTopicMetrics failed: {result}")
    
    def test_get_topic_statistics(self):
        """测试获取Topic统计信息接口"""
        print("Testing getTopicStatistics...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic"
        }
        
        success, result = self.make_request("/topic/getTopicStatistics", data)
        
        if success:
            print("✓ getTopicStatistics - Success")
        else:
            print(f"✗ getTopicStatistics - {result}")
        
        self.assertTrue(success, f"getTopicStatistics failed: {result}")
    
    def test_update_topic_config(self):
        """测试更新Topic配置接口"""
        print("Testing updateTopicConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "config": {
                "perm": 6,
                "readQueueNums": 8,
                "writeQueueNums": 8
            }
        }
        
        success, result = self.make_request("/topic/updateTopicConfig", data)
        
        if success:
            print("✓ updateTopicConfig - Success")
        else:
            print(f"✗ updateTopicConfig - {result}")
        
        self.assertTrue(success, f"updateTopicConfig failed: {result}")
    
    def test_search_topic_by_sub_key(self):
        """测试根据订阅Key搜索Topic接口"""
        print("Testing searchTopicBySubKey...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "subKey": "test_sub_key"
        }
        
        success, result = self.make_request("/topic/searchTopicBySubKey", data)
        
        if success:
            print("✓ searchTopicBySubKey - Success")
        else:
            print(f"✗ searchTopicBySubKey - {result}")
        
        self.assertTrue(success, f"searchTopicBySubKey failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("Topic Tool Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()