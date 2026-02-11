#!/usr/bin/env python3
"""
RocketMQ MCP HTTP接口测试脚本 - unittest版本
使用unittest框架测试所有tool类的HTTP接口功能
"""

import unittest
import requests
import json
import time
from datetime import datetime
import sys

class TestRocketMQMCP(unittest.TestCase):
    """RocketMQ MCP HTTP接口测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 设置基础URL
        cls.base_url = "http://localhost:6868"
        if len(sys.argv) > 1:
            cls.base_url = sys.argv[1]
        
        cls.session = requests.Session()
        cls.results = []
        
        print(f"Testing RocketMQ MCP server at: {cls.base_url}")
        print("=" * 60)
    
    def setUp(self):
        """每个测试方法前的准备工作"""
        self.start_time = time.time()
    
    def tearDown(self):
        """每个测试方法后的清理工作"""
        response_time = time.time() - self.start_time
        # 记录测试执行时间
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
    
    def test_controller_endpoints(self):
        """测试Controller相关接口"""
        print("\nTesting Controller Endpoints")
        
        # 测试获取控制器配置
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "controllers": ["127.0.0.1:10911"]
        }
        success, result = self.make_request("/controller/getControllerConfig", data)
        
        if success:
            print("✓ Controller.getControllerConfig - Success")
        else:
            print(f"✗ Controller.getControllerConfig - {result}")
        
        # 这里我们使用assertTrue来验证请求成功
        # 在实际环境中，可能需要根据具体响应内容进行更详细的断言
        self.assertTrue(success, f"Controller endpoint failed: {result}")
    
    def test_nameserver_endpoints(self):
        """测试Nameserver相关接口"""
        print("\nTesting Nameserver Endpoints")
        
        # 测试获取NameServer地址列表
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        success, result = self.make_request("/nameserver/getNameServerAddressList", data)
        
        if success:
            print("✓ Nameserver.getNameServerAddressList - Success")
        else:
            print(f"✗ Nameserver.getNameServerAddressList - {result}")
        
        self.assertTrue(success, f"Nameserver endpoint failed: {result}")
        
        # 测试获取KV配置
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "namespace": "test",
            "key": "test_key"
        }
        success, result = self.make_request("/nameserver/getKVConfig", data)
        
        if success:
            print("✓ Nameserver.getKVConfig - Success")
        else:
            print(f"✗ Nameserver.getKVConfig - {result}")
        
        self.assertTrue(success, f"Nameserver KV config endpoint failed: {result}")
    
    def test_message_endpoints(self):
        """测试Message相关接口"""
        print("\nTesting Message Endpoints")
        
        # 测试查询消息
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "msgId": "test_msg_id"
        }
        success, result = self.make_request("/message/viewMessage", data)
        
        if success:
            print("✓ Message.viewMessage - Success")
        else:
            print(f"✗ Message.viewMessage - {result}")
        
        self.assertTrue(success, f"Message endpoint failed: {result}")
    
    def test_broker_endpoints(self):
        """测试Broker相关接口"""
        print("\nTesting Broker Endpoints")
        
        # 测试获取Broker运行时统计
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "brokerAddr": "127.0.0.1:10911"
        }
        success, result = self.make_request("/broker/getBrokerRuntimeStats", data)
        
        if success:
            print("✓ Broker.getBrokerRuntimeStats - Success")
        else:
            print(f"✗ Broker.getBrokerRuntimeStats - {result}")
        
        self.assertTrue(success, f"Broker endpoint failed: {result}")
    
    def test_acl_endpoints(self):
        """测试ACL相关接口"""
        print("\nTesting ACL Endpoints")
        
        # 测试获取ACL版本列表
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "addr": "127.0.0.1:10911"
        }
        success, result = self.make_request("/acl/getAclVersionList", data)
        
        if success:
            print("✓ Acl.getAclVersionList - Success")
        else:
            print(f"✗ Acl.getAclVersionList - {result}")
        
        self.assertTrue(success, f"ACL endpoint failed: {result}")
    
    def test_consumer_endpoints(self):
        """测试Consumer相关接口"""
        print("\nTesting Consumer Endpoints")
        
        # 测试获取消费者连接信息
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "consumerGroup": "test_group"
        }
        success, result = self.make_request("/consumer/examineConsumerConnectionInfo", data)
        
        if success:
            print("✓ Consumer.examineConsumerConnectionInfo - Success")
        else:
            print(f"✗ Consumer.examineConsumerConnectionInfo - {result}")
        
        self.assertTrue(success, f"Consumer endpoint failed: {result}")
    
    def test_topic_endpoints(self):
        """测试Topic相关接口"""
        print("\nTesting Topic Endpoints")
        
        # 测试获取Topic列表
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        success, result = self.make_request("/topic/topicList", data)
        
        if success:
            print("✓ Topic.topicList - Success")
        else:
            print(f"✗ Topic.topicList - {result}")
        
        self.assertTrue(success, f"Topic endpoint failed: {result}")
    
    def test_cluster_endpoints(self):
        """测试Cluster相关接口"""
        print("\nTesting Cluster Endpoints")
        
        # 测试获取集群信息
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test"
        }
        success, result = self.make_request("/cluster/clusterList", data)
        
        if success:
            print("✓ Cluster.clusterList - Success")
        else:
            print(f"✗ Cluster.clusterList - {result}")
        
        self.assertTrue(success, f"Cluster endpoint failed: {result}")
    
    def test_producer_endpoints(self):
        """测试Producer相关接口"""
        print("\nTesting Producer Endpoints")
        
        # 测试获取生产者连接信息
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "producerGroup": "test_group"
        }
        success, result = self.make_request("/producer/examineProducerConnectionInfo", data)
        
        if success:
            print("✓ Producer.examineProducerConnectionInfo - Success")
        else:
            print(f"✗ Producer.examineProducerConnectionInfo - {result}")
        
        self.assertTrue(success, f"Producer endpoint failed: {result}")
    
    def test_consumequeue_endpoints(self):
        """测试ConsumeQueue相关接口"""
        print("\nTesting ConsumeQueue Endpoints")
        
        # 测试获取消费队列信息
        data = {
            "nameserverAddressList": ["127.0.0.1:9876"],
            "ak": "test",
            "sk": "test",
            "topic": "test_topic",
            "brokerAddr": "127.0.0.1:10911"
        }
        success, result = self.make_request("/consumequeue/examineConsumeQueue", data)
        
        if success:
            print("✓ ConsumeQueue.examineConsumeQueue - Success")
        else:
            print(f"✗ ConsumeQueue.examineConsumeQueue - {result}")
        
        self.assertTrue(success, f"ConsumeQueue endpoint failed: {result}")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理工作"""
        print("\n" + "=" * 60)
        print("Test execution completed")
        print("=" * 60)


def main():
    """主函数 - 支持命令行参数"""
    # 支持命令行参数传递服务器地址
    if len(sys.argv) > 1:
        # 将参数传递给unittest
        unittest.main(argv=[sys.argv[0]] + sys.argv[1:])
    else:
        unittest.main()


if __name__ == "__main__":
    main()