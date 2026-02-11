package org.apache.rocketmq.mcp.tool;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.mcp.common.ApiResponse;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

/**
 * 利用rocketmq的admin接口提供rocketmq集群信息管理服务
 */
@org.springframework.stereotype.Service
public class Consumer {
    public static final List<String> WRITE_OPERATIONS = List.of(
            "createAndUpdateSubscriptionGroupConfig",
            "createAndUpdateSubscriptionGroupConfigList",
            "resetConsumerOffset",
            "deleteSubscriptionGroup",
            "exportPopRecords",
            "resetOffsetByTimestampOld",
            "setMessageRequestMode",
            "resetOffsetByQueueId",
            "updateAndGetGroupReadForbidden",
            "resetOffsetNew",
            "cloneGroupOffset",
            "updateConsumeOffset"
    );

    @Tool(description = "获取消费者组信息")
    public ApiResponse<List<SubscriptionGroupConfig>> examineSubscriptionGroupConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                                     @ToolParam(description = "access key or ak") String ak,
                                                                                     @ToolParam(description = "secret key or sk") String sk,
                                                                                     @ToolParam(description = "消费者组名称") String group) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                List<SubscriptionGroupConfig> list = new ArrayList<>();
                for (String brokerAddr : Broker.getAllBrokerAddresses(admin)) {
                    list.add(admin.examineSubscriptionGroupConfig(brokerAddr, group));
                }
                return list;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组列表")
    public ApiResponse<List<SubscriptionGroupWrapper>> getAllSubscriptionGroup(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                               @ToolParam(description = "access key or ak") String ak,
                                                                               @ToolParam(description = "secret key or sk") String sk) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                List<SubscriptionGroupWrapper> list = new ArrayList<>();
                for (String brokerAddr : Broker.getAllBrokerAddresses(admin)) {
                    list.add(admin.getAllSubscriptionGroup(brokerAddr, 30000));
                }
                return list;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费进度")
    public ApiResponse<Object> examineConsumeStats(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "消费者组名称") String group) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineConsumeStats(group);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者堆栈信息, 运行状态")
    public ApiResponse<Object> getConsumeJstack(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "消费者组") String consumerGroup,
                                                @ToolParam(description = "客户端id") String clientId) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getConsumerRunningInfo(consumerGroup, clientId, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建和更新订阅组配置")
    public ApiResponse<String> createAndUpdateSubscriptionGroupConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                      @ToolParam(description = "access key or ak") String ak,
                                                                      @ToolParam(description = "secret key or sk") String sk,
                                                                      @ToolParam(description = "broker地址") String addr,
                                                                      @ToolParam(description = "订阅组配置") SubscriptionGroupConfig config) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.createAndUpdateSubscriptionGroupConfig(addr, config);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建和更新订阅组配置列表")
    public ApiResponse<String> createAndUpdateSubscriptionGroupConfigList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                          @ToolParam(description = "access key or ak") String ak,
                                                                          @ToolParam(description = "secret key or sk") String sk,
                                                                          @ToolParam(description = "broker地址") String brokerAddr,
                                                                          @ToolParam(description = "订阅组配置列表") List<SubscriptionGroupConfig> configs) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.createAndUpdateSubscriptionGroupConfigList(brokerAddr, configs);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(按主题)")
    public ApiResponse<Object> examineConsumeStatsByTopic(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                          @ToolParam(description = "access key or ak") String ak,
                                                          @ToolParam(description = "secret key or sk") String sk,
                                                          @ToolParam(description = "消费者组名称") String consumerGroup,
                                                          @ToolParam(description = "主题名称") String topic) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineConsumeStats(consumerGroup, topic);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(按集群)")
    public ApiResponse<Object> examineConsumeStatsByCluster(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                            @ToolParam(description = "access key or ak") String ak,
                                                            @ToolParam(description = "secret key or sk") String sk,
                                                            @ToolParam(description = "集群名称") String clusterName,
                                                            @ToolParam(description = "消费者组名称") String consumerGroup,
                                                            @ToolParam(description = "主题名称") String topic) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineConsumeStats(clusterName, consumerGroup, topic);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(带超时)")
    public ApiResponse<Object> examineConsumeStatsWithTimeout(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                              @ToolParam(description = "access key or ak") String ak,
                                                              @ToolParam(description = "secret key or sk") String sk,
                                                              @ToolParam(description = "broker地址") String brokerAddr,
                                                              @ToolParam(description = "消费者组名称") String consumerGroup,
                                                              @ToolParam(description = "主题名称") String topicName,
                                                              @ToolParam(description = "超时时间(毫秒)") long timeoutMillis) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineConsumeStats(brokerAddr, consumerGroup, topicName, timeoutMillis);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(并发)")
    public ApiResponse<Object> examineConsumeStatsConcurrent(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                             @ToolParam(description = "access key or ak") String ak,
                                                             @ToolParam(description = "secret key or sk") String sk,
                                                             @ToolParam(description = "消费者组名称") String consumerGroup,
                                                             @ToolParam(description = "主题名称") String topic) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineConsumeStatsConcurrent(consumerGroup, topic);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者连接信息")
    public ApiResponse<Object> examineConsumerConnectionInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                             @ToolParam(description = "access key or ak") String ak,
                                                             @ToolParam(description = "secret key or sk") String sk,
                                                             @ToolParam(description = "消费者组名称") String consumerGroup) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineConsumerConnectionInfo(consumerGroup);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者连接信息(按broker)")
    public ApiResponse<Object> examineConsumerConnectionInfoByBroker(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                     @ToolParam(description = "access key or ak") String ak,
                                                                     @ToolParam(description = "secret key or sk") String sk,
                                                                     @ToolParam(description = "消费者组名称") String consumerGroup,
                                                                     @ToolParam(description = "broker地址") String brokerAddr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineConsumerConnectionInfo(consumerGroup, brokerAddr);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "触发消费者重平衡")
    public ApiResponse<String> resetConsumerOffset(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "消费者组") String consumerGroup,
                                                   @ToolParam(description = "主题名称") String topic,
                                                   @ToolParam(description = "时间戳") long timestamp) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.resetOffsetByTimestamp(topic, consumerGroup, timestamp, true);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除订阅组")
    public ApiResponse<String> deleteSubscriptionGroup(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                       @ToolParam(description = "access key or ak") String ak,
                                                       @ToolParam(description = "secret key or sk") String sk,
                                                       @ToolParam(description = "broker地址") String brokerAddr,
                                                       @ToolParam(description = "消费者组") String consumerGroup) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.deleteSubscriptionGroup(brokerAddr, consumerGroup);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者连接信息(带broker地址)")
    public ApiResponse<Object> examineConsumerConnectionInfoWithAddr(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                     @ToolParam(description = "access key or ak") String ak,
                                                                     @ToolParam(description = "secret key or sk") String sk,
                                                                     @ToolParam(description = "消费者组") String consumerGroup) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineConsumerConnectionInfo(consumerGroup);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "导出POP记录")
    public ApiResponse<String> exportPopRecords(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "broker地址") String brokerAddr,
                                                @ToolParam(description = "超时时间") long timeout) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.exportPopRecords(brokerAddr, timeout);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "重置消费者偏移量(旧版)")
    public ApiResponse<String> resetOffsetByTimestampOld(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                         @ToolParam(description = "access key or ak") String ak,
                                                         @ToolParam(description = "secret key or sk") String sk,
                                                         @ToolParam(description = "消费者组") String consumerGroup,
                                                         @ToolParam(description = "主题") String topic,
                                                         @ToolParam(description = "时间戳") long timestamp,
                                                         @ToolParam(description = "是否强制") boolean force) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询主题的消费者")
    public ApiResponse<Object> queryTopicConsumeByWho(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                      @ToolParam(description = "access key or ak") String ak,
                                                      @ToolParam(description = "secret key or sk") String sk,
                                                      @ToolParam(description = "主题") String topic) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.queryTopicConsumeByWho(topic);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消费者订阅的主题")
    public ApiResponse<Object> queryTopicsByConsumer(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                     @ToolParam(description = "access key or ak") String ak,
                                                     @ToolParam(description = "secret key or sk") String sk,
                                                     @ToolParam(description = "消费者组") String group) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.queryTopicsByConsumer(group);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询订阅信息")
    public ApiResponse<Object> querySubscription(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "消费者组") String group,
                                                 @ToolParam(description = "主题") String topic) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.querySubscription(group, topic);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消费时间跨度")
    public ApiResponse<Object> queryConsumeTimeSpan(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                    @ToolParam(description = "access key or ak") String ak,
                                                    @ToolParam(description = "secret key or sk") String sk,
                                                    @ToolParam(description = "主题") String topic,
                                                    @ToolParam(description = "消费者组") String group) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.queryConsumeTimeSpan(topic, group);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "设置消息请求模式")
    public ApiResponse<String> setMessageRequestMode(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                     @ToolParam(description = "access key or ak") String ak,
                                                     @ToolParam(description = "secret key or sk") String sk,
                                                     @ToolParam(description = "broker地址") String brokerAddr,
                                                     @ToolParam(description = "主题") String topic,
                                                     @ToolParam(description = "消费者组") String consumerGroup,
                                                     @ToolParam(description = "模式") String mode,
                                                     @ToolParam(description = "POP工作组大小") int popWorkGroupSize,
                                                     @ToolParam(description = "超时时间") long timeoutMillis) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.setMessageRequestMode(brokerAddr, topic, consumerGroup,
                        MessageRequestMode.valueOf(mode), popWorkGroupSize, timeoutMillis);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "重置队列偏移量")
    public ApiResponse<String> resetOffsetByQueueId(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                    @ToolParam(description = "access key or ak") String ak,
                                                    @ToolParam(description = "secret key or sk") String sk,
                                                    @ToolParam(description = "broker地址") String brokerAddr,
                                                    @ToolParam(description = "消费者组") String consumerGroup,
                                                    @ToolParam(description = "主题") String topicName,
                                                    @ToolParam(description = "队列ID") int queueId,
                                                    @ToolParam(description = "重置偏移量") long resetOffset) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.resetOffsetByQueueId(brokerAddr, consumerGroup, topicName, queueId, resetOffset);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新和获取组读权限")
    public ApiResponse<Object> updateAndGetGroupReadForbidden(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                              @ToolParam(description = "access key or ak") String ak,
                                                              @ToolParam(description = "secret key or sk") String sk,
                                                              @ToolParam(description = "broker地址") String brokerAddr,
                                                              @ToolParam(description = "消费者组") String groupName,
                                                              @ToolParam(description = "主题名称") String topicName,
                                                              @ToolParam(description = "是否可读") Boolean readable) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.updateAndGetGroupReadForbidden(brokerAddr, groupName, topicName, readable);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "重置消费者偏移量(新版)")
    public ApiResponse<String> resetOffsetNew(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                              @ToolParam(description = "access key or ak") String ak,
                                              @ToolParam(description = "secret key or sk") String sk,
                                              @ToolParam(description = "消费者组") String consumerGroup,
                                              @ToolParam(description = "主题") String topic,
                                              @ToolParam(description = "时间戳") long timestamp) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.resetOffsetNew(consumerGroup, topic, timestamp);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费状态")
    public ApiResponse<Object> getConsumeStatus(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "主题") String topic,
                                                @ToolParam(description = "消费者组") String group,
                                                @ToolParam(description = "客户端地址") String clientAddr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getConsumeStatus(topic, group, clientAddr);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "克隆消费者组偏移量")
    public ApiResponse<String> cloneGroupOffset(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "源消费者组") String srcGroup,
                                                @ToolParam(description = "目标消费者组") String destGroup,
                                                @ToolParam(description = "主题") String topic,
                                                @ToolParam(description = "是否离线") boolean isOffline) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.cloneGroupOffset(srcGroup, destGroup, topic, isOffline);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker消费统计")
    public ApiResponse<Object> fetchConsumeStatsInBroker(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                         @ToolParam(description = "access key or ak") String ak,
                                                         @ToolParam(description = "secret key or sk") String sk,
                                                         @ToolParam(description = "broker地址") String brokerAddr,
                                                         @ToolParam(description = "是否顺序消费") boolean isOrder,
                                                         @ToolParam(description = "超时时间") long timeoutMillis) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.fetchConsumeStatsInBroker(brokerAddr, isOrder, timeoutMillis);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取用户订阅组")
    public ApiResponse<Object> getUserSubscriptionGroup(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                        @ToolParam(description = "access key or ak") String ak,
                                                        @ToolParam(description = "secret key or sk") String sk,
                                                        @ToolParam(description = "broker地址") String brokerAddr,
                                                        @ToolParam(description = "超时时间") long timeoutMillis) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getUserSubscriptionGroup(brokerAddr, timeoutMillis);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新消费偏移量")
    public ApiResponse<String> updateConsumeOffset(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "broker地址") String brokerAddr,
                                                   @ToolParam(description = "消费者组") String consumeGroup,
                                                   @ToolParam(description = "消息队列") String mqJson,
                                                   @ToolParam(description = "偏移量") long offset) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                MessageQueue mq = com.alibaba.fastjson2.JSON.parseObject(mqJson, MessageQueue.class);
                admin.updateConsumeOffset(brokerAddr, consumeGroup, mq, offset);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

}