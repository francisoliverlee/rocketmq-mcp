#!/usr/bin/env python3
"""
Controller工具单元测试
测试Controller相关的HTTP接口功能
"""

import unittest
import requests
import json
import time
from datetime import datetime
import sys


class TestControllerTool(unittest.TestCase):
    """Controller工具单元测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 设置基础URL
        cls.base_url = "http://localhost:6868"
        if len(sys.argv) > 1:
            cls.base_url = sys.argv[1]
        
        cls.session = requests.Session()
        
        print(f"Testing Controller Tool at: {cls.base_url}")
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
    
    def test_update_controller_config(self):
        """测试更新控制器配置接口"""
        print("Testing updateControllerConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "properties": {
                "configKey1": "configValue1",
                "configKey2": "configValue2"
            },
            "controllers": ["127.0.0.1:10911"]
        }
        
        success, result = self.make_request("/controller/updateControllerConfig", data)
        
        if success:
            print("✓ updateControllerConfig - Success")
        else:
            print(f"✗ updateControllerConfig - {result}")
        
        # 对于写操作，我们主要测试接口是否可访问
        self.assertTrue(success, f"updateControllerConfig failed: {result}")
    
    def test_get_controller_config(self):
        """测试获取控制器配置接口"""
        print("Testing getControllerConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "controllers": ["127.0.0.1:10911"]
        }
        
        success, result = self.make_request("/controller/getControllerConfig", data)
        
        if success:
            print("✓ getControllerConfig - Success")
        else:
            print(f"✗ getControllerConfig - {result}")
        
        self.assertTrue(success, f"getControllerConfig failed: {result}")
    
    def test_elect_master(self):
        """测试选举主节点接口"""
        print("Testing electMaster...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "controllerAddr": "127.0.0.1:10911",
            "brokerName": "test_broker",
            "clusterName": "test_cluster",
            "brokerId": 0
        }
        
        success, result = self.make_request("/controller/electMaster", data)
        
        if success:
            print("✓ electMaster - Success")
        else:
            print(f"✗ electMaster - {result}")
        
        self.assertTrue(success, f"electMaster failed: {result}")
    
    def test_clean_expired_controller(self):
        """测试清理过期控制器接口"""
        print("Testing cleanExpiredController...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "controllerAddr": "127.0.0.1:10911",
            "clusterName": "test_cluster",
            "brokerName": "test_broker",
            "brokerControllerIdsToClean": "1,2,3",
            "isCleanLivingBroker": False
        }
        
        success, result = self.make_request("/controller/cleanExpiredController", data)
        
        if success:
            print("✓ cleanExpiredController - Success")
        else:
            print(f"✗ cleanExpiredController - {result}")
        
        self.assertTrue(success, f"cleanExpiredController failed: {result}")
    
    def test_clean_controller_broker_data(self):
        """测试清理控制器broker元数据接口"""
        print("Testing cleanControllerBrokerData...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "controllerAddr": "127.0.0.1:10911",
            "clusterName": "test_cluster",
            "brokerName": "test_broker",
            "brokerControllerIdsToClean": "1,2,3",
            "isCleanLivingBroker": False
        }
        
        success, result = self.make_request("/controller/cleanControllerBrokerData", data)
        
        if success:
            print("✓ cleanControllerBrokerData - Success")
        else:
            print(f"✗ cleanControllerBrokerData - {result}")
        
        self.assertTrue(success, f"cleanControllerBrokerData failed: {result}")
    
    def test_get_in_sync_state_data(self):
        """测试获取同步状态数据接口"""
        print("Testing getInSyncStateData...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "controllerAddress": "127.0.0.1:10911",
            "brokers": ["broker1", "broker2"]
        }
        
        success, result = self.make_request("/controller/getInSyncStateData", data)
        
        if success:
            print("✓ getInSyncStateData - Success")
        else:
            print(f"✗ getInSyncStateData - {result}")
        
        self.assertTrue(success, f"getInSyncStateData failed: {result}")
    
    def test_get_controller_meta_data(self):
        """测试获取控制器元数据接口"""
        print("Testing getControllerMetaData...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "controllerAddr": "127.0.0.1:10911"
        }
        
        success, result = self.make_request("/controller/getControllerMetaData", data)
        
        if success:
            print("✓ getControllerMetaData - Success")
        else:
            print(f"✗ getControllerMetaData - {result}")
        
        self.assertTrue(success, f"getControllerMetaData failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("Controller Tool Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()