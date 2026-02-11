#!/usr/bin/env python3
"""
RocketMQ MCP 单元测试主运行脚本
统一运行所有分类的单元测试文件
"""

import unittest
import sys
import os
import time
from datetime import datetime

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入所有测试模块
from test_controller_tool import TestControllerTool
from test_broker_tool import TestBrokerTool
from test_consumer_tool import TestConsumerTool
from test_message_tool import TestMessageTool
from test_nameserver_tool import TestNameserverTool
from test_topic_tool import TestTopicTool
from test_acl_tool import TestAclTool
from test_cluster_tool import TestClusterTool
from test_producer_tool import TestProducerTool
from test_consumequeue_tool import TestConsumeQueueTool


def run_all_tests(base_url=None):
    """运行所有测试"""
    print("=" * 70)
    print("RocketMQ MCP 单元测试套件")
    print("=" * 70)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试服务器: {base_url or 'http://localhost:6868'}")
    print("=" * 70)
    
    # 设置基础URL
    if base_url:
        os.environ['BASE_URL'] = base_url
    
    # 创建测试套件
    test_suite = unittest.TestSuite()
    
    # 添加所有测试类
    test_classes = [
        TestControllerTool,
        TestBrokerTool,
        TestConsumerTool,
        TestMessageTool,
        TestNameserverTool,
        TestTopicTool,
        TestAclTool,
        TestClusterTool,
        TestProducerTool,
        TestConsumeQueueTool
    ]
    
    for test_class in test_classes:
        # 为每个测试类设置基础URL
        if base_url:
            test_class.base_url = base_url
        
        # 获取测试方法
        test_names = unittest.defaultTestLoader.getTestCaseNames(test_class)
        for test_name in test_names:
            test_suite.addTest(test_class(test_name))
    
    # 运行测试
    runner = unittest.TextTestRunner(
        verbosity=2,
        descriptions=True,
        failfast=False
    )
    
    start_time = time.time()
    result = runner.run(test_suite)
    end_time = time.time()
    
    # 输出测试报告
    print("\n" + "=" * 70)
    print("测试报告")
    print("=" * 70)
    print(f"运行时间: {end_time - start_time:.2f} 秒")
    print(f"测试用例数: {result.testsRun}")
    print(f"通过数: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失败数: {len(result.failures)}")
    print(f"错误数: {len(result.errors)}")
    
    if result.failures:
        print("\n失败用例:")
        for test, traceback in result.failures:
            print(f"  - {test}")
    
    if result.errors:
        print("\n错误用例:")
        for test, traceback in result.errors:
            print(f"  - {test}")
    
    print("=" * 70)
    
    return result


def run_specific_category(category, base_url=None):
    """运行特定分类的测试"""
    print(f"\n运行 {category} 分类测试")
    print("=" * 50)
    
    # 分类映射
    category_map = {
        'controller': TestControllerTool,
        'broker': TestBrokerTool,
        'consumer': TestConsumerTool,
        'message': TestMessageTool,
        'nameserver': TestNameserverTool,
        'topic': TestTopicTool,
        'acl': TestAclTool,
        'cluster': TestClusterTool,
        'producer': TestProducerTool,
        'consumequeue': TestConsumeQueueTool
    }
    
    if category not in category_map:
        print(f"错误: 未知的分类 '{category}'")
        print("可用分类:", ", ".join(category_map.keys()))
        return None
    
    test_class = category_map[category]
    
    # 设置基础URL
    if base_url:
        test_class.base_url = base_url
    
    # 创建测试套件
    test_suite = unittest.TestSuite()
    test_names = unittest.defaultTestLoader.getTestCaseNames(test_class)
    for test_name in test_names:
        test_suite.addTest(test_class(test_name))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result


def list_categories():
    """列出所有可用的测试分类"""
    categories = {
        'controller': 'Controller工具测试',
        'broker': 'Broker工具测试',
        'consumer': 'Consumer工具测试',
        'message': 'Message工具测试',
        'nameserver': 'Nameserver工具测试',
        'topic': 'Topic工具测试',
        'acl': 'ACL工具测试',
        'cluster': 'Cluster工具测试',
        'producer': 'Producer工具测试',
        'consumequeue': 'ConsumeQueue工具测试'
    }
    
    print("可用测试分类:")
    for key, description in categories.items():
        print(f"  {key}: {description}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='RocketMQ MCP 单元测试运行器')
    parser.add_argument('--url', '-u', default='http://localhost:6868',
                        help='测试服务器URL (默认: http://localhost:6868)')
    parser.add_argument('--category', '-c', choices=[
        'controller', 'broker', 'consumer', 'message', 'nameserver',
        'topic', 'acl', 'cluster', 'producer', 'consumequeue'
    ], help='运行特定分类的测试')
    parser.add_argument('--list', '-l', action='store_true',
                        help='列出所有可用的测试分类')
    
    args = parser.parse_args()
    
    if args.list:
        list_categories()
        return
    
    if args.category:
        run_specific_category(args.category, args.url)
    else:
        run_all_tests(args.url)


if __name__ == "__main__":
    main()