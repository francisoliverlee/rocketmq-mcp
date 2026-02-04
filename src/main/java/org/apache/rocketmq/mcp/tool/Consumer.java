package org.apache.rocketmq.mcp.tool;

import com.alibaba.fastjson2.JSON;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

/**
 * 利用rocketmq的admin接口提供rocketmq集群信息管理服务
 */
@org.springframework.stereotype.Service
public class Consumer {

    @Tool(description = "获取消费者组信息")
    public String examineSubscriptionGroupConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "消费者组名称") String group) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                List<SubscriptionGroupConfig> list = new ArrayList<>();
                for (String brokerAddr : Broker.getAllBrokerAddresses(admin)) {
                    list.add(admin.examineSubscriptionGroupConfig(brokerAddr, group));
                }
                return JSON.toJSONString(list);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组列表")
    public String getAllSubscriptionGroup(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                List<SubscriptionGroupWrapper> list = new ArrayList<>();
                for (String brokerAddr : Broker.getAllBrokerAddresses(admin)) {
                    list.add(admin.getAllSubscriptionGroup(brokerAddr, 30000));
                }
                return JSON.toJSONString(list);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "获取消费者组消费进度")
    public String examineConsumeStats(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "消费者组名称") String group) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumeStats(group));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者堆栈信息, 运行状态")
    public String getConsumeJstack(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "消费者组") String consumerGroup,
                                   @ToolParam(description = "客户端id") String clientId) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getConsumerRunningInfo(consumerGroup, clientId, true));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建和更新订阅组配置")
    public String createAndUpdateSubscriptionGroupConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                         @ToolParam(description = "access key or ak") String ak,
                                                         @ToolParam(description = "secret key or sk") String sk,
                                                         @ToolParam(description = "broker地址") String addr,
                                                         @ToolParam(description = "订阅组配置") SubscriptionGroupConfig config) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.createAndUpdateSubscriptionGroupConfig(addr, config);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建和更新订阅组配置列表")
    public String createAndUpdateSubscriptionGroupConfigList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                             @ToolParam(description = "access key or ak") String ak,
                                                             @ToolParam(description = "secret key or sk") String sk,
                                                             @ToolParam(description = "broker地址") String brokerAddr,
                                                             @ToolParam(description = "订阅组配置列表") List<SubscriptionGroupConfig> configs) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.createAndUpdateSubscriptionGroupConfigList(brokerAddr, configs);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "获取消费者组消费统计(按主题)")
    public String examineConsumeStatsByTopic(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                             @ToolParam(description = "access key or ak") String ak,
                                             @ToolParam(description = "secret key or sk") String sk,
                                             @ToolParam(description = "消费者组名称") String consumerGroup,
                                             @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumeStats(consumerGroup, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(按集群)")
    public String examineConsumeStatsByCluster(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                               @ToolParam(description = "access key or ak") String ak,
                                               @ToolParam(description = "secret key or sk") String sk,
                                               @ToolParam(description = "集群名称") String clusterName,
                                               @ToolParam(description = "消费者组名称") String consumerGroup,
                                               @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumeStats(clusterName, consumerGroup, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(带超时)")
    public String examineConsumeStatsWithTimeout(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "broker地址") String brokerAddr,
                                                 @ToolParam(description = "消费者组名称") String consumerGroup,
                                                 @ToolParam(description = "主题名称") String topicName,
                                                 @ToolParam(description = "超时时间(毫秒)") long timeoutMillis) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumeStats(brokerAddr, consumerGroup, topicName, timeoutMillis));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(并发)")
    public String examineConsumeStatsConcurrent(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "消费者组名称") String consumerGroup,
                                                @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumeStatsConcurrent(consumerGroup, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者连接信息")
    public String examineConsumerConnectionInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "消费者组名称") String consumerGroup) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumerConnectionInfo(consumerGroup));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者连接信息(按broker)")
    public String examineConsumerConnectionInfoByBroker(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                        @ToolParam(description = "access key or ak") String ak,
                                                        @ToolParam(description = "secret key or sk") String sk,
                                                        @ToolParam(description = "消费者组名称") String consumerGroup,
                                                        @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumerConnectionInfo(consumerGroup, brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "触发消费者重平衡")
    public String resetConsumerOffset(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "消费者组") String consumerGroup,
                                      @ToolParam(description = "主题名称") String topic,
                                      @ToolParam(description = "时间戳") long timestamp) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.resetOffsetByTimestamp(topic, consumerGroup, timestamp, true);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "删除订阅组")
    public String deleteSubscriptionGroup(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "broker地址") String brokerAddr,
                                          @ToolParam(description = "消费者组") String consumerGroup) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.deleteSubscriptionGroup(brokerAddr, consumerGroup);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者连接信息(带broker地址)")
    public String examineConsumerConnectionInfoWithAddr(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                        @ToolParam(description = "access key or ak") String ak,
                                                        @ToolParam(description = "secret key or sk") String sk,
                                                        @ToolParam(description = "消费者组") String consumerGroup) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumerConnectionInfo(consumerGroup));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "导出POP记录")
    public String exportPopRecords(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "broker地址") String brokerAddr,
                                   @ToolParam(description = "超时时间") long timeout) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.exportPopRecords(brokerAddr, timeout);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "重置消费者偏移量(旧版)")
    public String resetOffsetByTimestampOld(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "消费者组") String consumerGroup,
                                            @ToolParam(description = "主题") String topic,
                                            @ToolParam(description = "时间戳") long timestamp,
                                            @ToolParam(description = "是否强制") boolean force) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询主题的消费者")
    public String queryTopicConsumeByWho(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "主题") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryTopicConsumeByWho(topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消费者订阅的主题")
    public String queryTopicsByConsumer(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                        @ToolParam(description = "access key or ak") String ak,
                                        @ToolParam(description = "secret key or sk") String sk,
                                        @ToolParam(description = "消费者组") String group) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryTopicsByConsumer(group));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询订阅信息")
    public String querySubscription(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "消费者组") String group,
                                    @ToolParam(description = "主题") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.querySubscription(group, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消费时间跨度")
    public String queryConsumeTimeSpan(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "主题") String topic,
                                       @ToolParam(description = "消费者组") String group) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryConsumeTimeSpan(topic, group));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "设置消息请求模式")
    public String setMessageRequestMode(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                        @ToolParam(description = "access key or ak") String ak,
                                        @ToolParam(description = "secret key or sk") String sk,
                                        @ToolParam(description = "broker地址") String brokerAddr,
                                        @ToolParam(description = "主题") String topic,
                                        @ToolParam(description = "消费者组") String consumerGroup,
                                        @ToolParam(description = "模式") String mode,
                                        @ToolParam(description = "POP工作组大小") int popWorkGroupSize,
                                        @ToolParam(description = "超时时间") long timeoutMillis) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.setMessageRequestMode(brokerAddr, topic, consumerGroup,
                        MessageRequestMode.valueOf(mode), popWorkGroupSize, timeoutMillis);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "重置队列偏移量")
    public String resetOffsetByQueueId(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "broker地址") String brokerAddr,
                                       @ToolParam(description = "消费者组") String consumerGroup,
                                       @ToolParam(description = "主题") String topicName,
                                       @ToolParam(description = "队列ID") int queueId,
                                       @ToolParam(description = "重置偏移量") long resetOffset) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.resetOffsetByQueueId(brokerAddr, consumerGroup, topicName, queueId, resetOffset);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新和获取组读权限")
    public String updateAndGetGroupReadForbidden(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "broker地址") String brokerAddr,
                                                 @ToolParam(description = "消费者组") String groupName,
                                                 @ToolParam(description = "主题名称") String topicName,
                                                 @ToolParam(description = "是否可读") Boolean readable) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.updateAndGetGroupReadForbidden(brokerAddr, groupName, topicName, readable));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "重置消费者偏移量(新版)")
    public String resetOffsetNew(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                 @ToolParam(description = "access key or ak") String ak,
                                 @ToolParam(description = "secret key or sk") String sk,
                                 @ToolParam(description = "消费者组") String consumerGroup,
                                 @ToolParam(description = "主题") String topic,
                                 @ToolParam(description = "时间戳") long timestamp) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.resetOffsetNew(consumerGroup, topic, timestamp);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费状态")
    public String getConsumeStatus(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "主题") String topic,
                                   @ToolParam(description = "消费者组") String group,
                                   @ToolParam(description = "客户端地址") String clientAddr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getConsumeStatus(topic, group, clientAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "克隆消费者组偏移量")
    public String cloneGroupOffset(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "源消费者组") String srcGroup,
                                   @ToolParam(description = "目标消费者组") String destGroup,
                                   @ToolParam(description = "主题") String topic,
                                   @ToolParam(description = "是否离线") boolean isOffline) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.cloneGroupOffset(srcGroup, destGroup, topic, isOffline);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker消费统计")
    public String fetchConsumeStatsInBroker(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "broker地址") String brokerAddr,
                                            @ToolParam(description = "是否顺序消费") boolean isOrder,
                                            @ToolParam(description = "超时时间") long timeoutMillis) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.fetchConsumeStatsInBroker(brokerAddr, isOrder, timeoutMillis));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取用户订阅组")
    public String getUserSubscriptionGroup(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                           @ToolParam(description = "access key or ak") String ak,
                                           @ToolParam(description = "secret key or sk") String sk,
                                           @ToolParam(description = "broker地址") String brokerAddr,
                                           @ToolParam(description = "超时时间") long timeoutMillis) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getUserSubscriptionGroup(brokerAddr, timeoutMillis));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新消费偏移量")
    public String updateConsumeOffset(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "broker地址") String brokerAddr,
                                      @ToolParam(description = "消费者组") String consumeGroup,
                                      @ToolParam(description = "消息队列") String mqJson,
                                      @ToolParam(description = "偏移量") long offset) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                MessageQueue mq = JSON.parseObject(mqJson, MessageQueue.class);
                admin.updateConsumeOffset(brokerAddr, consumeGroup, mq, offset);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}
