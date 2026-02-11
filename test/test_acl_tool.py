#!/usr/bin/env python3
"""
ACL工具单元测试
测试ACL相关的HTTP接口功能
"""

import unittest
import requests
import json
import time
from datetime import datetime
import sys


class TestAclTool(unittest.TestCase):
    """ACL工具单元测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 设置基础URL
        cls.base_url = "http://localhost:6868"
        if len(sys.argv) > 1:
            cls.base_url = sys.argv[1]
        
        cls.session = requests.Session()
        
        print(f"Testing ACL Tool at: {cls.base_url}")
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
    
    def test_get_acl_version_list(self):
        """测试获取ACL版本列表接口"""
        print("Testing getAclVersionList...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "addr": "127.0.0.1:10911"
        }
        
        success, result = self.make_request("/acl/getAclVersionList", data)
        
        if success:
            print("✓ getAclVersionList - Success")
        else:
            print(f"✗ getAclVersionList - {result}")
        
        self.assertTrue(success, f"getAclVersionList failed: {result}")
    
    def test_get_broker_cluster_acl_info(self):
        """测试获取Broker集群ACL信息接口"""
        print("Testing getBrokerClusterAclInfo...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        
        success, result = self.make_request("/acl/getBrokerClusterAclInfo", data)
        
        if success:
            print("✓ getBrokerClusterAclInfo - Success")
        else:
            print(f"✗ getBrokerClusterAclInfo - {result}")
        
        self.assertTrue(success, f"getBrokerClusterAclInfo failed: {result}")
    
    def test_update_global_white_addrs(self):
        """测试更新全局白名单接口"""
        print("Testing updateGlobalWhiteAddrs...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "globalWhiteAddrs": ["127.0.0.1", "192.168.1.0/24"]
        }
        
        success, result = self.make_request("/acl/updateGlobalWhiteAddrs", data)
        
        if success:
            print("✓ updateGlobalWhiteAddrs - Success")
        else:
            print(f"✗ updateGlobalWhiteAddrs - {result}")
        
        self.assertTrue(success, f"updateGlobalWhiteAddrs failed: {result}")
    
    def test_delete_global_white_addrs(self):
        """测试删除全局白名单接口"""
        print("Testing deleteGlobalWhiteAddrs...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "globalWhiteAddrs": ["127.0.0.1"]
        }
        
        success, result = self.make_request("/acl/deleteGlobalWhiteAddrs", data)
        
        if success:
            print("✓ deleteGlobalWhiteAddrs - Success")
        else:
            print(f"✗ deleteGlobalWhiteAddrs - {result}")
        
        self.assertTrue(success, f"deleteGlobalWhiteAddrs failed: {result}")
    
    def test_reset_acl_config(self):
        """测试重置ACL配置接口"""
        print("Testing resetAclConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "addr": "127.0.0.1:10911"
        }
        
        success, result = self.make_request("/acl/resetAclConfig", data)
        
        if success:
            print("✓ resetAclConfig - Success")
        else:
            print(f"✗ resetAclConfig - {result}")
        
        self.assertTrue(success, f"resetAclConfig failed: {result}")
    
    def test_get_acls(self):
        """测试获取ACL列表接口"""
        print("Testing getAcls...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "addr": "127.0.0.1:10911"
        }
        
        success, result = self.make_request("/acl/getAcls", data)
        
        if success:
            print("✓ getAcls - Success")
        else:
            print(f"✗ getAcls - {result}")
        
        self.assertTrue(success, f"getAcls failed: {result}")
    
    def test_update_acl_config(self):
        """测试更新ACL配置接口"""
        print("Testing updateAclConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "addr": "127.0.0.1:10911",
            "aclConfig": {
                "globalWhiteAddrs": ["127.0.0.1"],
                "accounts": [
                    {
                        "accessKey": "test_key",
                        "secretKey": "test_secret",
                        "whiteRemoteAddress": "127.0.0.1",
                        "admin": False
                    }
                ]
            }
        }
        
        success, result = self.make_request("/acl/updateAclConfig", data)
        
        if success:
            print("✓ updateAclConfig - Success")
        else:
            print(f"✗ updateAclConfig - {result}")
        
        self.assertTrue(success, f"updateAclConfig failed: {result}")
    
    def test_delete_acl_config(self):
        """测试删除ACL配置接口"""
        print("Testing deleteAclConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "addr": "127.0.0.1:10911"
        }
        
        success, result = self.make_request("/acl/deleteAclConfig", data)
        
        if success:
            print("✓ deleteAclConfig - Success")
        else:
            print(f"✗ deleteAclConfig - {result}")
        
        self.assertTrue(success, f"deleteAclConfig failed: {result}")
    
    def test_get_acl_metrics(self):
        """测试获取ACL指标接口"""
        print("Testing getAclMetrics...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "addr": "127.0.0.1:10911"
        }
        
        success, result = self.make_request("/acl/getAclMetrics", data)
        
        if success:
            print("✓ getAclMetrics - Success")
        else:
            print(f"✗ getAclMetrics - {result}")
        
        self.assertTrue(success, f"getAclMetrics failed: {result}")
    
    def test_get_acl_statistics(self):
        """测试获取ACL统计信息接口"""
        print("Testing getAclStatistics...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        
        success, result = self.make_request("/acl/getAclStatistics", data)
        
        if success:
            print("✓ getAclStatistics - Success")
        else:
            print(f"✗ getAclStatistics - {result}")
        
        self.assertTrue(success, f"getAclStatistics failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("ACL Tool Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()