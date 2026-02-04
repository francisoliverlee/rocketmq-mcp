package org.apache.rocketmq.mcp.tool;

import com.alibaba.fastjson2.JSON;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

/**
 * 利用rocketmq的admin接口提供rocketmq集群信息管理服务
 */
@org.springframework.stereotype.Service
public class Topic {
    @Tool(description = "获取主题列表")
    public String fetchAllTopicList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.fetchAllTopicList());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取主题统计信息")
    public String examineTopicStats(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineTopicStats(topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取主题路由信息")
    public String examineTopicRouteInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineTopicRouteInfo(topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "获取主题配置信息")
    public String examineTopicConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "broker地址") String brokerAddr, @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineTopicConfig(brokerAddr, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建主题")
    public String createAndUpdateTopicConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "broker地址") String brokerAddr, @ToolParam(description = "主题名称") String topic, @ToolParam(description = "队列数量") int queueNum) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                TopicConfig topicConfig = new TopicConfig();
                topicConfig.setTopicName(topic);
                topicConfig.setReadQueueNums(queueNum);
                topicConfig.setWriteQueueNums(queueNum);
                admin.createAndUpdateTopicConfig(brokerAddr, topicConfig);
                return admin.examineTopicConfig(brokerAddr, topic) != null ? "success" : "fail";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除主题")
    public String deleteTopicInBroker(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "broker地址") String brokerAddr, @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.deleteTopicInBroker(new HashSet<String>() {
                    {
                        add(brokerAddr);
                    }
                }, topic);
                return admin.examineTopicConfig(brokerAddr, topic) == null ? "success" : "fail";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建和更新主题配置列表")
    public String createAndUpdateTopicConfigList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "broker地址") String addr, @ToolParam(description = "主题配置列表") List<TopicConfig> topicConfigList) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.createAndUpdateTopicConfigList(addr, topicConfigList);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取集群列表")
    public String fetchTopicsByCLuster(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "集群名称") String clusterName) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.fetchTopicsByCLuster(clusterName));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理未使用的主题")
    public String cleanUnusedTopic(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "集群名称") String cluster) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return String.valueOf(admin.cleanUnusedTopic(cluster));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建静态主题")
    public String createStaticTopic(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "broker地址") String addr, @ToolParam(description = "默认主题") String defaultTopic, @ToolParam(description = "主题配置") TopicConfig topicConfig, @ToolParam(description = "队列映射详情") String mappingDetail, @ToolParam(description = "是否强制") boolean force) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                TopicQueueMappingDetail detail = JSON.parseObject(mappingDetail, TopicQueueMappingDetail.class);
                admin.createStaticTopic(addr, defaultTopic, topicConfig, detail, force);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除主题(按集群)")
    public String deleteTopic(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "主题名称") String topicName, @ToolParam(description = "集群名称") String clusterName) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.deleteTopic(topicName, clusterName);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除主题(在NameServer中)")
    public String deleteTopicInNameServer(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "nameserver地址列表") Set<String> addrs, @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.deleteTopicInNameServer(addrs, topic);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除主题(在NameServer中带集群)")
    public String deleteTopicInNameServerWithCluster(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "nameserver地址列表") Set<String> addrs, @ToolParam(description = "集群名称") String clusterName, @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.deleteTopicInNameServer(addrs, clusterName, topic);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取主题集群列表")
    public String getTopicClusterList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "主题") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getTopicClusterList(topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取用户主题配置")
    public String getUserTopicConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList, @ToolParam(description = "access key or ak") String ak, @ToolParam(description = "secret key or sk") String sk, @ToolParam(description = "broker地址") String brokerAddr, @ToolParam(description = "是否特殊主题") boolean specialTopic, @ToolParam(description = "超时时间") long timeoutMillis) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getUserTopicConfig(brokerAddr, specialTopic, timeoutMillis));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}
