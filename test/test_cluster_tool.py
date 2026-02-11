#!/usr/bin/env python3
"""
Cluster工具单元测试
测试Cluster相关的HTTP接口功能
"""

import unittest
import requests
import json
import time
from datetime import datetime
import sys


class TestClusterTool(unittest.TestCase):
    """Cluster工具单元测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 设置基础URL
        cls.base_url = "http://localhost:6868"
        if len(sys.argv) > 1:
            cls.base_url = sys.argv[1]
        
        cls.session = requests.Session()
        
        print(f"Testing Cluster Tool at: {cls.base_url}")
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
    
    def test_cluster_list(self):
        """测试获取集群列表接口"""
        print("Testing clusterList...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        
        success, result = self.make_request("/cluster/clusterList", data)
        
        if success:
            print("✓ clusterList - Success")
        else:
            print(f"✗ clusterList - {result}")
        
        self.assertTrue(success, f"clusterList failed: {result}")
    
    def test_get_cluster_info(self):
        """测试获取集群信息接口"""
        print("Testing getClusterInfo...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "clusterName": "test_cluster"
        }
        
        success, result = self.make_request("/cluster/getClusterInfo", data)
        
        if success:
            print("✓ getClusterInfo - Success")
        else:
            print(f"✗ getClusterInfo - {result}")
        
        self.assertTrue(success, f"getClusterInfo failed: {result}")
    
    def test_get_cluster_config(self):
        """测试获取集群配置接口"""
        print("Testing getClusterConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "clusterName": "test_cluster"
        }
        
        success, result = self.make_request("/cluster/getClusterConfig", data)
        
        if success:
            print("✓ getClusterConfig - Success")
        else:
            print(f"✗ getClusterConfig - {result}")
        
        self.assertTrue(success, f"getClusterConfig failed: {result}")
    
    def test_update_cluster_config(self):
        """测试更新集群配置接口"""
        print("Testing updateClusterConfig...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "clusterName": "test_cluster",
            "config": {
                "brokerName": "test_broker",
                "clusterName": "test_cluster",
                "brokerId": 0
            }
        }
        
        success, result = self.make_request("/cluster/updateClusterConfig", data)
        
        if success:
            print("✓ updateClusterConfig - Success")
        else:
            print(f"✗ updateClusterConfig - {result}")
        
        self.assertTrue(success, f"updateClusterConfig failed: {result}")
    
    def test_get_cluster_metrics(self):
        """测试获取集群指标接口"""
        print("Testing getClusterMetrics...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "clusterName": "test_cluster"
        }
        
        success, result = self.make_request("/cluster/getClusterMetrics", data)
        
        if success:
            print("✓ getClusterMetrics - Success")
        else:
            print(f"✗ getClusterMetrics - {result}")
        
        self.assertTrue(success, f"getClusterMetrics failed: {result}")
    
    def test_get_cluster_statistics(self):
        """测试获取集群统计信息接口"""
        print("Testing getClusterStatistics...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "clusterName": "test_cluster"
        }
        
        success, result = self.make_request("/cluster/getClusterStatistics", data)
        
        if success:
            print("✓ getClusterStatistics - Success")
        else:
            print(f"✗ getClusterStatistics - {result}")
        
        self.assertTrue(success, f"getClusterStatistics failed: {result}")
    
    def test_examine_cluster_health(self):
        """测试检查集群健康状态接口"""
        print("Testing examineClusterHealth...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "clusterName": "test_cluster"
        }
        
        success, result = self.make_request("/cluster/examineClusterHealth", data)
        
        if success:
            print("✓ examineClusterHealth - Success")
        else:
            print(f"✗ examineClusterHealth - {result}")
        
        self.assertTrue(success, f"examineClusterHealth failed: {result}")
    
    def test_get_cluster_topology(self):
        """测试获取集群拓扑结构接口"""
        print("Testing getClusterTopology...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "clusterName": "test_cluster"
        }
        
        success, result = self.make_request("/cluster/getClusterTopology", data)
        
        if success:
            print("✓ getClusterTopology - Success")
        else:
            print(f"✗ getClusterTopology - {result}")
        
        self.assertTrue(success, f"getClusterTopology failed: {result}")
    
    def test_get_cluster_members(self):
        """测试获取集群成员列表接口"""
        print("Testing getClusterMembers...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "clusterName": "test_cluster"
        }
        
        success, result = self.make_request("/cluster/getClusterMembers", data)
        
        if success:
            print("✓ getClusterMembers - Success")
        else:
            print(f"✗ getClusterMembers - {result}")
        
        self.assertTrue(success, f"getClusterMembers failed: {result}")
    
    def test_get_cluster_performance(self):
        """测试获取集群性能指标接口"""
        print("Testing getClusterPerformance...")
        
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "clusterName": "test_cluster"
        }
        
        success, result = self.make_request("/cluster/getClusterPerformance", data)
        
        if success:
            print("✓ getClusterPerformance - Success")
        else:
            print(f"✗ getClusterPerformance - {result}")
        
        self.assertTrue(success, f"getClusterPerformance failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("Cluster Tool Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()