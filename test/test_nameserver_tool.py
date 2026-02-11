#!/usr/bin/env python3
"""
Nameserver工具单元测试
测试Nameserver相关的HTTP接口功能
"""

import unittest
import requests
import json
import time
from datetime import datetime
import sys


class TestNameserverTool(unittest.TestCase):
    """Nameserver工具单元测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 设置基础URL
        cls.base_url = "http://localhost:6868"
        if len(sys.argv) > 1:
            cls.base_url = sys.argv[1]
        
        cls.session = requests.Session()
        
        print(f"Testing Nameserver Tool at: {cls.base_url}")
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
    
    def test_get_nameserver_address_list(self):
        """测试获取NameServer地址列表接口"""
        print("Testing getNameServerAddressList...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        
        success, result = self.make_request("/nameserver/getNameServerAddressList", data)
        
        if success:
            print("✓ getNameServerAddressList - Success")
        else:
            print(f"✗ getNameServerAddressList - {result}")
        
        self.assertTrue(success, f"getNameServerAddressList failed: {result}")
    
    def test_get_kv_config(self):
        """测试获取KV配置接口"""
        print("Testing getKVConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "namespace": "test_namespace",
            "key": "test_key"
        }
        
        success, result = self.make_request("/nameserver/getKVConfig", data)
        
        if success:
            print("✓ getKVConfig - Success")
        else:
            print(f"✗ getKVConfig - {result}")
        
        self.assertTrue(success, f"getKVConfig failed: {result}")
    
    def test_get_kv_config_by_namespace(self):
        """测试根据命名空间获取KV配置接口"""
        print("Testing getKVConfigByNamespace...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "namespace": "test_namespace"
        }
        
        success, result = self.make_request("/nameserver/getKVConfigByNamespace", data)
        
        if success:
            print("✓ getKVConfigByNamespace - Success")
        else:
            print(f"✗ getKVConfigByNamespace - {result}")
        
        self.assertTrue(success, f"getKVConfigByNamespace failed: {result}")
    
    def test_get_kv_list_by_namespace(self):
        """测试获取命名空间下的KV列表接口"""
        print("Testing getKVListByNamespace...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "namespace": "test_namespace"
        }
        
        success, result = self.make_request("/nameserver/getKVListByNamespace", data)
        
        if success:
            print("✓ getKVListByNamespace - Success")
        else:
            print(f"✗ getKVListByNamespace - {result}")
        
        self.assertTrue(success, f"getKVListByNamespace failed: {result}")
    
    def test_delete_kv_config(self):
        """测试删除KV配置接口"""
        print("Testing deleteKVConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "namespace": "test_namespace",
            "key": "test_key"
        }
        
        success, result = self.make_request("/nameserver/deleteKVConfig", data)
        
        if success:
            print("✓ deleteKVConfig - Success")
        else:
            print(f"✗ deleteKVConfig - {result}")
        
        self.assertTrue(success, f"deleteKVConfig failed: {result}")
    
    def test_get_project_group_by_ip(self):
        """测试根据IP获取项目组接口"""
        print("Testing getProjectGroupByIp...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "ip": "127.0.0.1"
        }
        
        success, result = self.make_request("/nameserver/getProjectGroupByIp", data)
        
        if success:
            print("✓ getProjectGroupByIp - Success")
        else:
            print(f"✗ getProjectGroupByIp - {result}")
        
        self.assertTrue(success, f"getProjectGroupByIp failed: {result}")
    
    def test_get_system_topic_list(self):
        """测试获取系统Topic列表接口"""
        print("Testing getSystemTopicList...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        
        success, result = self.make_request("/nameserver/getSystemTopicList", data)
        
        if success:
            print("✓ getSystemTopicList - Success")
        else:
            print(f"✗ getSystemTopicList - {result}")
        
        self.assertTrue(success, f"getSystemTopicList failed: {result}")
    
    def test_get_unit_topic_list(self):
        """测试获取单元Topic列表接口"""
        print("Testing getUnitTopicList...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        
        success, result = self.make_request("/nameserver/getUnitTopicList", data)
        
        if success:
            print("✓ getUnitTopicList - Success")
        else:
            print(f"✗ getUnitTopicList - {result}")
        
        self.assertTrue(success, f"getUnitTopicList failed: {result}")
    
    def test_get_has_unit_sub_topic_list(self):
        """测试获取有单元订阅的Topic列表接口"""
        print("Testing getHasUnitSubTopicList...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        
        success, result = self.make_request("/nameserver/getHasUnitSubTopicList", data)
        
        if success:
            print("✓ getHasUnitSubTopicList - Success")
        else:
            print(f"✗ getHasUnitSubTopicList - {result}")
        
        self.assertTrue(success, f"getHasUnitSubTopicList failed: {result}")
    
    def test_get_has_unit_sub_unit_topic_list(self):
        """测试获取有单元订阅的单元Topic列表接口"""
        print("Testing getHasUnitSubUnitTopicList...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        
        success, result = self.make_request("/nameserver/getHasUnitSubUnitTopicList", data)
        
        if success:
            print("✓ getHasUnitSubUnitTopicList - Success")
        else:
            print(f"✗ getHasUnitSubUnitTopicList - {result}")
        
        self.assertTrue(success, f"getHasUnitSubUnitTopicList failed: {result}")
    
    def test_get_topic_route_info(self):
        """测试获取Topic路由信息接口"""
        print("Testing getTopicRouteInfo...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic"
        }
        
        success, result = self.make_request("/nameserver/getTopicRouteInfo", data)
        
        if success:
            print("✓ getTopicRouteInfo - Success")
        else:
            print(f"✗ getTopicRouteInfo - {result}")
        
        self.assertTrue(success, f"getTopicRouteInfo failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("Nameserver Tool Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()