#!/usr/bin/env python3
"""
Producer工具单元测试
测试Producer相关的HTTP接口功能
"""

import unittest
import requests
import json
import time
from datetime import datetime
import sys


class TestProducerTool(unittest.TestCase):
    """Producer工具单元测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 设置基础URL
        cls.base_url = "http://localhost:6868"
        if len(sys.argv) > 1:
            cls.base_url = sys.argv[1]
        
        cls.session = requests.Session()
        
        print(f"Testing Producer Tool at: {cls.base_url}")
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
    
    def test_examine_producer_connection_info(self):
        """测试检查生产者连接信息接口"""
        print("Testing examineProducerConnectionInfo...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "producerGroup": "test_group"
        }
        
        success, result = self.make_request("/producer/examineProducerConnectionInfo", data)
        
        if success:
            print("✓ examineProducerConnectionInfo - Success")
        else:
            print(f"✗ examineProducerConnectionInfo - {result}")
        
        self.assertTrue(success, f"examineProducerConnectionInfo failed: {result}")
    
    def test_get_producer_list(self):
        """测试获取生产者列表接口"""
        print("Testing getProducerList...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        
        success, result = self.make_request("/producer/getProducerList", data)
        
        if success:
            print("✓ getProducerList - Success")
        else:
            print(f"✗ getProducerList - {result}")
        
        self.assertTrue(success, f"getProducerList failed: {result}")
    
    def test_get_producer_metrics(self):
        """测试获取生产者指标接口"""
        print("Testing getProducerMetrics...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "producerGroup": "test_group"
        }
        
        success, result = self.make_request("/producer/getProducerMetrics", data)
        
        if success:
            print("✓ getProducerMetrics - Success")
        else:
            print(f"✗ getProducerMetrics - {result}")
        
        self.assertTrue(success, f"getProducerMetrics failed: {result}")
    
    def test_get_producer_statistics(self):
        """测试获取生产者统计信息接口"""
        print("Testing getProducerStatistics...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "producerGroup": "test_group"
        }
        
        success, result = self.make_request("/producer/getProducerStatistics", data)
        
        if success:
            print("✓ getProducerStatistics - Success")
        else:
            print(f"✗ getProducerStatistics - {result}")
        
        self.assertTrue(success, f"getProducerStatistics failed: {result}")
    
    def test_get_producer_config(self):
        """测试获取生产者配置接口"""
        print("Testing getProducerConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "producerGroup": "test_group"
        }
        
        success, result = self.make_request("/producer/getProducerConfig", data)
        
        if success:
            print("✓ getProducerConfig - Success")
        else:
            print(f"✗ getProducerConfig - {result}")
        
        self.assertTrue(success, f"getProducerConfig failed: {result}")
    
    def test_update_producer_config(self):
        """测试更新生产者配置接口"""
        print("Testing updateProducerConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "producerGroup": "test_group",
            "config": {
                "sendMsgTimeout": 3000,
                "compressMsgBodyOverHowmuch": 1024,
                "retryTimesWhenSendFailed": 2
            }
        }
        
        success, result = self.make_request("/producer/updateProducerConfig", data)
        
        if success:
            print("✓ updateProducerConfig - Success")
        else:
            print(f"✗ updateProducerConfig - {result}")
        
        self.assertTrue(success, f"updateProducerConfig failed: {result}")
    
    def test_get_producer_health(self):
        """测试获取生产者健康状态接口"""
        print("Testing getProducerHealth...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "producerGroup": "test_group"
        }
        
        success, result = self.make_request("/producer/getProducerHealth", data)
        
        if success:
            print("✓ getProducerHealth - Success")
        else:
            print(f"✗ getProducerHealth - {result}")
        
        self.assertTrue(success, f"getProducerHealth failed: {result}")
    
    def test_get_producer_performance(self):
        """测试获取生产者性能指标接口"""
        print("Testing getProducerPerformance...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "producerGroup": "test_group"
        }
        
        success, result = self.make_request("/producer/getProducerPerformance", data)
        
        if success:
            print("✓ getProducerPerformance - Success")
        else:
            print(f"✗ getProducerPerformance - {result}")
        
        self.assertTrue(success, f"getProducerPerformance failed: {result}")
    
    def test_reset_producer_offset(self):
        """测试重置生产者偏移量接口"""
        print("Testing resetProducerOffset...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "producerGroup": "test_group",
            "topic": "test_topic"
        }
        
        success, result = self.make_request("/producer/resetProducerOffset", data)
        
        if success:
            print("✓ resetProducerOffset - Success")
        else:
            print(f"✗ resetProducerOffset - {result}")
        
        self.assertTrue(success, f"resetProducerOffset failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("Producer Tool Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()