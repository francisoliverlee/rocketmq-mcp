#!/usr/bin/env python3
"""
RocketMQ MCP unittest演示脚本
展示unittest框架的各种功能和使用方法
"""

import unittest
import sys
import os

# 添加当前目录到Python路径，以便导入测试模块
sys.path.insert(0, os.path.dirname(__file__))

class TestUnittestFeatures(unittest.TestCase):
    """演示unittest框架的各种功能"""
    
    @classmethod
    def setUpClass(cls):
        """类级别设置，在所有测试前执行一次"""
        print("=== setUpClass - 类级别初始化 ===")
        cls.shared_data = "共享数据"
        print(f"初始化共享数据: {cls.shared_data}")
    
    @classmethod
    def tearDownClass(cls):
        """类级别清理，在所有测试后执行一次"""
        print("\n=== tearDownClass - 类级别清理 ===")
        print("清理共享资源")
    
    def setUp(self):
        """方法级别设置，在每个测试方法前执行"""
        print(f"\n=== setUp - 准备测试 {self._testMethodName} ===")
        self.test_data = {"test": "data"}
    
    def tearDown(self):
        """方法级别清理，在每个测试方法后执行"""
        print(f"=== tearDown - 清理测试 {self._testMethodName} ===")
    
    def test_basic_assertions(self):
        """演示基本断言方法"""
        print("演示基本断言方法")
        
        # assertEqual - 检查相等性
        self.assertEqual(1 + 1, 2, "1+1应该等于2")
        
        # assertTrue/assertFalse - 检查布尔值
        self.assertTrue(True, "True应该为真")
        self.assertFalse(False, "False应该为假")
        
        # assertIsNone/assertIsNotNone - 检查None值
        self.assertIsNone(None, "值应该为None")
        self.assertIsNotNone("not none", "值不应该为None")
        
        # assertIn/assertNotIn - 检查包含关系
        self.assertIn("a", ["a", "b", "c"], "a应该在列表中")
        self.assertNotIn("d", ["a", "b", "c"], "d不应该在列表中")
    
    def test_exception_assertions(self):
        """演示异常断言"""
        print("演示异常断言")
        
        # assertRaises - 检查是否抛出异常
        with self.assertRaises(ValueError):
            int("not a number")
        
        # assertRaisesRegex - 检查异常消息匹配正则表达式
        with self.assertRaisesRegex(ValueError, "invalid literal"):
            int("abc")
    
    def test_skip_decorators(self):
        """演示跳过测试的装饰器"""
        print("演示跳过测试的装饰器")
        
        # @unittest.skip - 无条件跳过
        self.skipTest("演示无条件跳过")
    
    @unittest.skip("跳过这个测试 - 演示装饰器用法")
    def test_skipped_method(self):
        """这个测试会被跳过"""
        self.fail("这个测试不应该被执行")
    
    @unittest.skipIf(sys.platform == "win32", "在Windows上跳过")
    def test_conditional_skip(self):
        """条件跳过测试"""
        print("这个测试只在非Windows平台上运行")
        self.assertEqual(1, 1)
    
    def test_with_subtest(self):
        """演示子测试功能"""
        print("演示子测试功能")
        
        # 使用subtest进行参数化测试
        test_cases = [
            (1, 1, 2),
            (2, 3, 5),
            (5, 5, 10)
        ]
        
        for a, b, expected in test_cases:
            with self.subTest(a=a, b=b, expected=expected):
                result = a + b
                self.assertEqual(result, expected, 
                               f"{a} + {b} 应该等于 {expected}")
    
    def test_shared_data(self):
        """演示类级别共享数据"""
        print("演示类级别共享数据")
        self.assertEqual(self.shared_data, "共享数据")
        self.assertEqual(self.test_data["test"], "data")


class TestRocketMQMCPIntegration(unittest.TestCase):
    """演示RocketMQ MCP集成测试"""
    
    def test_server_connectivity(self):
        """测试服务器连接性"""
        print("测试服务器连接性")
        
        # 这里可以集成实际的RocketMQ MCP测试
        # 为了演示，我们使用模拟测试
        server_accessible = True  # 模拟服务器可访问
        
        self.assertTrue(server_accessible, "服务器应该可访问")
        print("✓ 服务器连接测试通过")
    
    def test_api_endpoints(self):
        """测试API端点"""
        print("测试API端点")
        
        # 模拟测试各种API端点
        endpoints = [
            "/controller/getControllerConfig",
            "/nameserver/getNameServerAddressList",
            "/message/viewMessage",
            "/broker/getBrokerRuntimeStats"
        ]
        
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                # 模拟API调用成功
                api_success = True
                self.assertTrue(api_success, f"API端点 {endpoint} 应该成功响应")
                print(f"✓ {endpoint} - 成功")


def run_demo_tests():
    """运行演示测试"""
    print("=" * 60)
    print("RocketMQ MCP unittest演示")
    print("=" * 60)
    
    # 创建测试套件
    suite = unittest.TestSuite()
    
    # 添加测试类 - 使用TestLoader替代过时的makeSuite
    loader = unittest.TestLoader()
    suite.addTest(loader.loadTestsFromTestCase(TestUnittestFeatures))
    suite.addTest(loader.loadTestsFromTestCase(TestRocketMQMCPIntegration))
    
    # 运行测试套件
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # 输出测试结果统计
    print("\n" + "=" * 60)
    print("测试结果统计:")
    print(f"运行测试数: {result.testsRun}")
    print(f"失败数: {len(result.failures)}")
    print(f"错误数: {len(result.errors)}")
    print(f"跳过数: {len(result.skipped)}")
    print("=" * 60)
    
    return result.wasSuccessful()


def main():
    """主函数"""
    # 运行演示测试
    success = run_demo_tests()
    
    # 输出使用说明
    print("\n使用说明:")
    print("1. 运行所有测试: python3 -m unittest test_rocketmq_mcp_unittest.py")
    print("2. 运行特定测试类: python3 -m unittest test_rocketmq_mcp_unittest.TestRocketMQMCP")
    print("3. 运行单个测试: python3 -m unittest test_rocketmq_mcp_unittest.TestRocketMQMCP.test_nameserver_endpoints")
    print("4. 详细输出: python3 -m unittest test_rocketmq_mcp_unittest.TestRocketMQMCP -v")
    print("5. 发现并运行所有测试: python3 -m unittest discover test/")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()