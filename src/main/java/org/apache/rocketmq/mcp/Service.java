package org.apache.rocketmq.mcp;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

import java.util.*;
import java.util.function.Function;

/**
 * 利用rocketmq的admin接口提供rocketmq集群信息管理服务
 */
@org.springframework.stereotype.Service
public class Service {
    private static String DEFAULT_NAME_SERVER = System.getProperty("NS_ADDR", System.getenv("NS_ADDR"));
    private static String DEFAULT_AK = System.getProperty("AK", System.getenv("AK"));
    private static String DEFAULT_SK = System.getProperty("SK", System.getenv("SK"));

    private String callAdmin(Function<DefaultMQAdminExt, String> func, String ak, String sk, String nameserverAddressList) throws MQClientException {
        String _ns = (nameserverAddressList == null || nameserverAddressList.trim().isEmpty()) ? DEFAULT_NAME_SERVER : nameserverAddressList.trim();
        String _ak = (ak == null || ak.trim().isEmpty()) ? DEFAULT_AK : ak.trim();
        String _sk = (sk == null || sk.trim().isEmpty()) ? DEFAULT_SK : sk.trim();
        DefaultMQAdminExt admin = getAdmin(_ns, _ak, _sk);
        try {
            return func.apply(admin);
        } catch (Exception ex) {
            return ex.getMessage();
        } finally {
            admin.shutdown();
        }
    }

    private DefaultMQAdminExt getAdmin(String nameserverAddressList, String ak, String sk) throws MQClientException {
        DefaultMQAdminExt admin = null;
        if (StringUtils.isNotBlank(ak) && StringUtils.isNotBlank(sk)) {
            admin = new DefaultMQAdminExt(new AclClientRPCHook(new SessionCredentials(ak, sk)));
        } else {
            admin = new DefaultMQAdminExt();
        }
        admin.setNamesrvAddr(nameserverAddressList);
        admin.start();
        return admin;
    }

    private List<String> getAllBrokerAddresses(DefaultMQAdminExt admin) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        List<String> result = new ArrayList<>();
        ClusterInfo clusterInfo = admin.examineBrokerClusterInfo();
        clusterInfo.getBrokerAddrTable().values().stream().forEach(brokerData -> {
            brokerData.getBrokerAddrs().values().stream().forEach(brokerAddr -> {
                result.add(brokerAddr);
            });
        });
        return result;
    }

    @Tool(description = "获取集群信息")
    public String examineBrokerClusterInfo(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                           @ToolParam(description = "access key or ak") String ak,
                                           @ToolParam(description = "secret key or sk") String sk) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineBrokerClusterInfo());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取主题列表")
    public String fetchAllTopicList(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.fetchAllTopicList());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取主题统计信息")
    public String examineTopicStats(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineTopicStats(topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组信息")
    public String examineSubscriptionGroupConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "消费者组名称") String group) throws MQClientException {
        return callAdmin(admin -> {
            try {
                List<SubscriptionGroupConfig> list = new ArrayList<>();
                for (String brokerAddr : getAllBrokerAddresses(admin)) {
                    list.add(admin.examineSubscriptionGroupConfig(brokerAddr, group));
                }
                return JSON.toJSONString(list);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取主题路由信息")
    public String examineTopicRouteInfo(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                        @ToolParam(description = "access key or ak") String ak,
                                        @ToolParam(description = "secret key or sk") String sk,
                                        @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineTopicRouteInfo(topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker统计信息")
    public String fetchBrokerRuntimeStats(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.fetchBrokerRuntimeStats(brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组列表")
    public String getAllSubscriptionGroup(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk) throws MQClientException {
        return callAdmin(admin -> {
            try {
                List<SubscriptionGroupWrapper> list = new ArrayList<>();
                for (String brokerAddr : getAllBrokerAddresses(admin)) {
                    list.add(admin.getAllSubscriptionGroup(brokerAddr, 30000));
                }
                return JSON.toJSONString(list);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取主题配置信息")
    public String examineTopicConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                     @ToolParam(description = "access key or ak") String ak,
                                     @ToolParam(description = "secret key or sk") String sk,
                                     @ToolParam(description = "broker地址") String brokerAddr,
                                     @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineTopicConfig(brokerAddr, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费进度")
    public String examineConsumeStats(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "消费者组名称") String group) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumeStats(group));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker配置信息")
    public String getBrokerConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                  @ToolParam(description = "access key or ak") String ak,
                                  @ToolParam(description = "secret key or sk") String sk,
                                  @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getBrokerConfig(brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者堆栈信息, 运行状态")
    public String getConsumeJstack(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "消费者组") String consumerGroup,
                                   @ToolParam(description = "客户端id") String clientId) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getConsumerRunningInfo(consumerGroup, clientId, true));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建主题")
    public String createAndUpdateTopicConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                             @ToolParam(description = "access key or ak") String ak,
                                             @ToolParam(description = "secret key or sk") String sk,
                                             @ToolParam(description = "broker地址") String brokerAddr,
                                             @ToolParam(description = "主题名称") String topic,
                                             @ToolParam(description = "队列数量") int queueNum) throws MQClientException {
        return callAdmin(admin -> {
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
    public String deleteTopicInBroker(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "broker地址") String brokerAddr,
                                      @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.deleteTopicInBroker(new HashSet<>() {
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

    @Tool(description = "更新Broker配置")
    public String updateBrokerConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                     @ToolParam(description = "access key or ak") String ak,
                                     @ToolParam(description = "secret key or sk") String sk,
                                     @ToolParam(description = "broker地址") String brokerAddr,
                                     @ToolParam(description = "配置项") String key,
                                     @ToolParam(description = "配置值") String value) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.updateBrokerConfig(brokerAddr, new Properties() {
                    {
                        put(key, value);
                    }
                });
                Object v = admin.getBrokerConfig(brokerAddr).getOrDefault(key, "");
                if (v == null) {
                    return "fail";
                }
                return v.toString().equalsIgnoreCase(value) ? "success" : "fail";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取全部broker地址列表")
    public String getAllBrokerAddresses(@ToolParam(description = "nameserver / namesrv 地址列表") String nameserverAddressList,
                                        @ToolParam(description = "access key or ak") String ak,
                                        @ToolParam(description = "secret key or sk") String sk) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(getAllBrokerAddresses(admin));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "添加broker到容器")
    public String addBrokerToContainer(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "broker容器地址") String brokerContainerAddr,
                                       @ToolParam(description = "broker配置") String brokerConfig) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.addBrokerToContainer(brokerContainerAddr, brokerConfig);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "从容器中移除broker")
    public String removeBrokerFromContainer(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "broker容器地址") String brokerContainerAddr,
                                            @ToolParam(description = "集群名称") String clusterName,
                                            @ToolParam(description = "broker名称") String brokerName,
                                            @ToolParam(description = "broker ID") long brokerId) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.removeBrokerFromContainer(brokerContainerAddr, clusterName, brokerName, brokerId);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建和更新主题配置列表")
    public String createAndUpdateTopicConfigList(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "broker地址") String addr,
                                                 @ToolParam(description = "主题配置列表") List<TopicConfig> topicConfigList) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.createAndUpdateTopicConfigList(addr, topicConfigList);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建和更新普通访问配置")
    public String createAndUpdatePlainAccessConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "broker地址") String addr,
                                                   @ToolParam(description = "普通访问配置") PlainAccessConfig plainAccessConfig) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.createAndUpdatePlainAccessConfig(addr, plainAccessConfig);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除普通访问配置")
    public String deletePlainAccessConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "broker地址") String addr,
                                          @ToolParam(description = "访问密钥") String accessKey) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.deletePlainAccessConfig(addr, accessKey);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新全局白名单地址配置")
    public String updateGlobalWhiteAddrConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                              @ToolParam(description = "access key or ak") String ak,
                                              @ToolParam(description = "secret key or sk") String sk,
                                              @ToolParam(description = "broker地址") String addr,
                                              @ToolParam(description = "全局白名单地址") String globalWhiteAddrs) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.updateGlobalWhiteAddrConfig(addr, globalWhiteAddrs);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新全局白名单地址配置(带ACL文件路径)")
    public String updateGlobalWhiteAddrConfigWithAcl(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                     @ToolParam(description = "access key or ak") String ak,
                                                     @ToolParam(description = "secret key or sk") String sk,
                                                     @ToolParam(description = "broker地址") String addr,
                                                     @ToolParam(description = "全局白名单地址") String globalWhiteAddrs,
                                                     @ToolParam(description = "ACL文件完整路径") String aclFileFullPath) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.updateGlobalWhiteAddrConfig(addr, globalWhiteAddrs, aclFileFullPath);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "检查broker集群ACL版本信息")
    public String examineBrokerClusterAclVersionInfo(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                     @ToolParam(description = "access key or ak") String ak,
                                                     @ToolParam(description = "secret key or sk") String sk,
                                                     @ToolParam(description = "broker地址") String addr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineBrokerClusterAclVersionInfo(addr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建和更新订阅组配置")
    public String createAndUpdateSubscriptionGroupConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                         @ToolParam(description = "access key or ak") String ak,
                                                         @ToolParam(description = "secret key or sk") String sk,
                                                         @ToolParam(description = "broker地址") String addr,
                                                         @ToolParam(description = "订阅组配置") SubscriptionGroupConfig config) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.createAndUpdateSubscriptionGroupConfig(addr, config);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建和更新订阅组配置列表")
    public String createAndUpdateSubscriptionGroupConfigList(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                             @ToolParam(description = "access key or ak") String ak,
                                                             @ToolParam(description = "secret key or sk") String sk,
                                                             @ToolParam(description = "broker地址") String brokerAddr,
                                                             @ToolParam(description = "订阅组配置列表") List<SubscriptionGroupConfig> configs) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.createAndUpdateSubscriptionGroupConfigList(brokerAddr, configs);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "检查RocksDB CQ写入进度")
    public String checkRocksdbCqWriteProgress(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                              @ToolParam(description = "access key or ak") String ak,
                                              @ToolParam(description = "secret key or sk") String sk,
                                              @ToolParam(description = "broker地址") String brokerAddr,
                                              @ToolParam(description = "主题名称") String topic,
                                              @ToolParam(description = "检查存储时间") long checkStoreTime) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.checkRocksdbCqWriteProgress(brokerAddr, topic, checkStoreTime));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(按主题)")
    public String examineConsumeStatsByTopic(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                             @ToolParam(description = "access key or ak") String ak,
                                             @ToolParam(description = "secret key or sk") String sk,
                                             @ToolParam(description = "消费者组名称") String consumerGroup,
                                             @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumeStats(consumerGroup, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(按集群)")
    public String examineConsumeStatsByCluster(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                               @ToolParam(description = "access key or ak") String ak,
                                               @ToolParam(description = "secret key or sk") String sk,
                                               @ToolParam(description = "集群名称") String clusterName,
                                               @ToolParam(description = "消费者组名称") String consumerGroup,
                                               @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumeStats(clusterName, consumerGroup, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(带超时)")
    public String examineConsumeStatsWithTimeout(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "broker地址") String brokerAddr,
                                                 @ToolParam(description = "消费者组名称") String consumerGroup,
                                                 @ToolParam(description = "主题名称") String topicName,
                                                 @ToolParam(description = "超时时间(毫秒)") long timeoutMillis) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumeStats(brokerAddr, consumerGroup, topicName, timeoutMillis));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者组消费统计(并发)")
    public String examineConsumeStatsConcurrent(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "消费者组名称") String consumerGroup,
                                                @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumeStatsConcurrent(consumerGroup, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者连接信息")
    public String examineConsumerConnectionInfo(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "消费者组名称") String consumerGroup) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumerConnectionInfo(consumerGroup));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者连接信息(按broker)")
    public String examineConsumerConnectionInfoByBroker(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                        @ToolParam(description = "access key or ak") String ak,
                                                        @ToolParam(description = "secret key or sk") String sk,
                                                        @ToolParam(description = "消费者组名称") String consumerGroup,
                                                        @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumerConnectionInfo(consumerGroup, brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取生产者连接信息")
    public String examineProducerConnectionInfo(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "生产者组名称") String producerGroup,
                                                @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineProducerConnectionInfo(producerGroup, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取所有生产者信息")
    public String getAllProducerInfo(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                     @ToolParam(description = "access key or ak") String ak,
                                     @ToolParam(description = "secret key or sk") String sk,
                                     @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getAllProducerInfo(brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取NameServer地址列表")
    public String getNameServerAddressList(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                           @ToolParam(description = "access key or ak") String ak,
                                           @ToolParam(description = "secret key or sk") String sk) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getNameServerAddressList());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "直接消费消息")
    public String consumeMessageDirectly(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "消费者组") String consumerGroup,
                                         @ToolParam(description = "topic") String topic,
                                         @ToolParam(description = "客户端ID") String clientId,
                                         @ToolParam(description = "消息ID") String msgId) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.consumeMessageDirectly(consumerGroup, clientId, topic, msgId));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "查询消息")
    public String viewMessage(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                              @ToolParam(description = "access key or ak") String ak,
                              @ToolParam(description = "secret key or sk") String sk,
                              @ToolParam(description = "主题名称") String topic,
                              @ToolParam(description = "消息ID") String msgId) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.viewMessage(topic, msgId));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "清理过期消息")
    public String cleanExpiredMessages(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.cleanExpiredConsumerQueue(brokerAddr);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "触发消费者重平衡")
    public String resetConsumerOffset(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "消费者组") String consumerGroup,
                                      @ToolParam(description = "主题名称") String topic,
                                      @ToolParam(description = "时间戳") long timestamp) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.resetOffsetByTimestamp(topic, consumerGroup, timestamp, true);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "删除订阅组")
    public String deleteSubscriptionGroup(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "broker地址") String brokerAddr,
                                          @ToolParam(description = "消费者组") String consumerGroup) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.deleteSubscriptionGroup(brokerAddr, consumerGroup);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "添加KV配置")
    public String putKVConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                              @ToolParam(description = "access key or ak") String ak,
                              @ToolParam(description = "secret key or sk") String sk,
                              @ToolParam(description = "命名空间") String namespace,
                              @ToolParam(description = "键") String key,
                              @ToolParam(description = "值") String value) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.putKVConfig(namespace, key, value);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取KV配置")
    public String getKVConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                              @ToolParam(description = "access key or ak") String ak,
                              @ToolParam(description = "secret key or sk") String sk,
                              @ToolParam(description = "命名空间") String namespace,
                              @ToolParam(description = "键") String key) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return admin.getKVConfig(namespace, key);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取命名空间下的KV列表")
    public String getKVListByNamespace(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "命名空间") String namespace) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getKVListByNamespace(namespace));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建或更新KV配置")
    public String createAndUpdateKvConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "命名空间") String namespace,
                                          @ToolParam(description = "键") String key,
                                          @ToolParam(description = "值") String value) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.createAndUpdateKvConfig(namespace, key, value);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除KV配置")
    public String deleteKvConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                 @ToolParam(description = "access key or ak") String ak,
                                 @ToolParam(description = "secret key or sk") String sk,
                                 @ToolParam(description = "命名空间") String namespace,
                                 @ToolParam(description = "键") String key) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.deleteKvConfig(namespace, key);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建或更新顺序配置")
    public String createOrUpdateOrderConf(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "键") String key,
                                          @ToolParam(description = "值") String value,
                                          @ToolParam(description = "是否集群") boolean isCluster) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.createOrUpdateOrderConf(key, value, isCluster);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消息")
    public String queryMessageByKey(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "主题") String topic,
                                    @ToolParam(description = "键") String key,
                                    @ToolParam(description = "最大消息数") int maxNum,
                                    @ToolParam(description = "开始时间") long begin,
                                    @ToolParam(description = "结束时间") long end) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryMessage(topic, key, maxNum, begin, end));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费者连接信息(带broker地址)")
    public String examineConsumerConnectionInfoWithAddr(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                        @ToolParam(description = "access key or ak") String ak,
                                                        @ToolParam(description = "secret key or sk") String sk,
                                                        @ToolParam(description = "消费者组") String consumerGroup) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineConsumerConnectionInfo(consumerGroup));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建用户")
    public String createUser(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                             @ToolParam(description = "access key or ak") String ak,
                             @ToolParam(description = "secret key or sk") String sk,
                             @ToolParam(description = "broker地址") String brokerAddr,
                             @ToolParam(description = "用户名") String username,
                             @ToolParam(description = "密码") String password,
                             @ToolParam(description = "用户类型") String userType) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.createUser(brokerAddr, username, password, userType);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新用户")
    public String updateUser(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                             @ToolParam(description = "access key or ak") String ak,
                             @ToolParam(description = "secret key or sk") String sk,
                             @ToolParam(description = "broker地址") String brokerAddr,
                             @ToolParam(description = "用户名") String username,
                             @ToolParam(description = "密码") String password,
                             @ToolParam(description = "用户类型") String userType,
                             @ToolParam(description = "用户状态") String userStatus) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.updateUser(brokerAddr, username, password, userType, userStatus);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除用户")
    public String deleteUser(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                             @ToolParam(description = "access key or ak") String ak,
                             @ToolParam(description = "secret key or sk") String sk,
                             @ToolParam(description = "broker地址") String brokerAddr,
                             @ToolParam(description = "用户名") String username) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.deleteUser(brokerAddr, username);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取用户信息")
    public String getUser(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                          @ToolParam(description = "access key or ak") String ak,
                          @ToolParam(description = "secret key or sk") String sk,
                          @ToolParam(description = "broker地址") String brokerAddr,
                          @ToolParam(description = "用户名") String username) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getUser(brokerAddr, username));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "列出用户")
    public String listUser(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                           @ToolParam(description = "access key or ak") String ak,
                           @ToolParam(description = "secret key or sk") String sk,
                           @ToolParam(description = "broker地址") String brokerAddr,
                           @ToolParam(description = "过滤条件") String filter) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.listUser(brokerAddr, filter));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建ACL")
    public String createAcl(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                            @ToolParam(description = "access key or ak") String ak,
                            @ToolParam(description = "secret key or sk") String sk,
                            @ToolParam(description = "broker地址") String brokerAddr,
                            @ToolParam(description = "主体") String subject,
                            @ToolParam(description = "资源列表") List<String> resources,
                            @ToolParam(description = "操作列表") List<String> actions,
                            @ToolParam(description = "源IP列表") List<String> sourceIps,
                            @ToolParam(description = "决策") String decision) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.createAcl(brokerAddr, subject, resources, actions, sourceIps, decision);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新ACL")
    public String updateAcl(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                            @ToolParam(description = "access key or ak") String ak,
                            @ToolParam(description = "secret key or sk") String sk,
                            @ToolParam(description = "broker地址") String brokerAddr,
                            @ToolParam(description = "主体") String subject,
                            @ToolParam(description = "资源列表") List<String> resources,
                            @ToolParam(description = "操作列表") List<String> actions,
                            @ToolParam(description = "源IP列表") List<String> sourceIps,
                            @ToolParam(description = "决策") String decision) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.updateAcl(brokerAddr, subject, resources, actions, sourceIps, decision);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "删除ACL")
    public String deleteAcl(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                            @ToolParam(description = "access key or ak") String ak,
                            @ToolParam(description = "secret key or sk") String sk,
                            @ToolParam(description = "broker地址") String brokerAddr,
                            @ToolParam(description = "主体") String subject,
                            @ToolParam(description = "资源") String resource) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.deleteAcl(brokerAddr, subject, resource);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取ACL信息")
    public String getAcl(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                         @ToolParam(description = "access key or ak") String ak,
                         @ToolParam(description = "secret key or sk") String sk,
                         @ToolParam(description = "broker地址") String brokerAddr,
                         @ToolParam(description = "主体") String subject) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getAcl(brokerAddr, subject));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "列出ACL")
    public String listAcl(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                          @ToolParam(description = "access key or ak") String ak,
                          @ToolParam(description = "secret key or sk") String sk,
                          @ToolParam(description = "broker地址") String brokerAddr,
                          @ToolParam(description = "主体过滤条件") String subjectFilter,
                          @ToolParam(description = "资源过滤条件") String resourceFilter) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.listAcl(brokerAddr, subjectFilter, resourceFilter));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "导出POP记录")
    public String exportPopRecords(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "broker地址") String brokerAddr,
                                   @ToolParam(description = "超时时间") long timeout) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.exportPopRecords(brokerAddr, timeout);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取集群列表")
    public String fetchTopicsByCLuster(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "集群名称") String clusterName) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.fetchTopicsByCLuster(clusterName));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "重置消费者偏移量(旧版)")
    public String resetOffsetByTimestampOld(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "消费者组") String consumerGroup,
                                            @ToolParam(description = "主题") String topic,
                                            @ToolParam(description = "时间戳") long timestamp,
                                            @ToolParam(description = "是否强制") boolean force) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询主题的消费者")
    public String queryTopicConsumeByWho(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "主题") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryTopicConsumeByWho(topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消费者订阅的主题")
    public String queryTopicsByConsumer(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                        @ToolParam(description = "access key or ak") String ak,
                                        @ToolParam(description = "secret key or sk") String sk,
                                        @ToolParam(description = "消费者组") String group) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryTopicsByConsumer(group));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询订阅信息")
    public String querySubscription(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "消费者组") String group,
                                    @ToolParam(description = "主题") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.querySubscription(group, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消费时间跨度")
    public String queryConsumeTimeSpan(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "主题") String topic,
                                       @ToolParam(description = "消费者组") String group) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryConsumeTimeSpan(topic, group));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理未使用的主题")
    public String cleanUnusedTopic(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "集群名称") String cluster) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return String.valueOf(admin.cleanUnusedTopic(cluster));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "重置主节点刷新偏移量")
    public String resetMasterFlushOffset(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "broker地址") String brokerAddr,
                                         @ToolParam(description = "主节点刷新偏移量") long masterFlushOffset) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.resetMasterFlushOffset(brokerAddr, masterFlushOffset);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消息(按集群)")
    public String queryMessageById(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "集群名称") String clusterName,
                                   @ToolParam(description = "主题") String topic,
                                   @ToolParam(description = "消息ID") String msgId) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryMessage(clusterName, topic, msgId));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker HA状态")
    public String getBrokerHAStatus(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getBrokerHAStatus(brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker Epoch缓存")
    public String getBrokerEpochCache(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getBrokerEpochCache(brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "设置CommitLog预读模式")
    public String setCommitLogReadAheadMode(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "broker地址") String brokerAddr,
                                            @ToolParam(description = "模式") String mode) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return admin.setCommitLogReadAheadMode(brokerAddr, mode);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取冷数据流控制信息")
    public String getColdDataFlowCtrInfo(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return admin.getColdDataFlowCtrInfo(brokerAddr);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新冷数据流控制组配置")
    public String updateColdDataFlowCtrGroupConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "broker地址") String brokerAddr,
                                                   @ToolParam(description = "配置属性") Properties properties) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.updateColdDataFlowCtrGroupConfig(brokerAddr, properties);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "移除冷数据流控制组配置")
    public String removeColdDataFlowCtrGroupConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "broker地址") String brokerAddr,
                                                   @ToolParam(description = "消费者组") String consumerGroup) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.removeColdDataFlowCtrGroupConfig(brokerAddr, consumerGroup);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消费队列数据")
    public String queryConsumeQueue(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "broker地址") String brokerAddr,
                                    @ToolParam(description = "主题") String topic,
                                    @ToolParam(description = "队列ID") int queueId,
                                    @ToolParam(description = "起始索引") long index,
                                    @ToolParam(description = "数量") int count,
                                    @ToolParam(description = "消费者组") String consumerGroup) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryConsumeQueue(brokerAddr, topic, queueId, index, count, consumerGroup));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "导出RocksDB配置到JSON")
    public String exportRocksDBConfigToJson(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "broker地址") String brokerAddr,
                                            @ToolParam(description = "配置类型列表") List<String> configTypes) throws MQClientException {
        return callAdmin(admin -> {
            try {
                List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> types = configTypes.stream()
                        .map(ExportRocksDBConfigToJsonRequestHeader.ConfigType::valueOf)
                        .toList();
                admin.exportRocksDBConfigToJson(brokerAddr, types);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "恢复检查半消息")
    public String resumeCheckHalfMessage(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "主题") String topic,
                                         @ToolParam(description = "消息ID") String msgId) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return String.valueOf(admin.resumeCheckHalfMessage(topic, msgId));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "设置消息请求模式")
    public String setMessageRequestMode(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                        @ToolParam(description = "access key or ak") String ak,
                                        @ToolParam(description = "secret key or sk") String sk,
                                        @ToolParam(description = "broker地址") String brokerAddr,
                                        @ToolParam(description = "主题") String topic,
                                        @ToolParam(description = "消费者组") String consumerGroup,
                                        @ToolParam(description = "模式") String mode,
                                        @ToolParam(description = "POP工作组大小") int popWorkGroupSize,
                                        @ToolParam(description = "超时时间") long timeoutMillis) throws MQClientException {
        return callAdmin(admin -> {
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
    public String resetOffsetByQueueId(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "broker地址") String brokerAddr,
                                       @ToolParam(description = "消费者组") String consumerGroup,
                                       @ToolParam(description = "主题") String topicName,
                                       @ToolParam(description = "队列ID") int queueId,
                                       @ToolParam(description = "重置偏移量") long resetOffset) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.resetOffsetByQueueId(brokerAddr, consumerGroup, topicName, queueId, resetOffset);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建静态主题")
    public String createStaticTopic(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "broker地址") String addr,
                                    @ToolParam(description = "默认主题") String defaultTopic,
                                    @ToolParam(description = "主题配置") TopicConfig topicConfig,
                                    @ToolParam(description = "队列映射详情") String mappingDetail,
                                    @ToolParam(description = "是否强制") boolean force) throws MQClientException {
        return callAdmin(admin -> {
            try {
                TopicQueueMappingDetail detail = JSON.parseObject(mappingDetail, TopicQueueMappingDetail.class);
                admin.createStaticTopic(addr, defaultTopic, topicConfig, detail, force);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新控制器配置")
    public String updateControllerConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "配置属性") Properties properties,
                                         @ToolParam(description = "控制器地址列表") List<String> controllers) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.updateControllerConfig(properties, controllers);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取控制器配置")
    public String getControllerConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "控制器地址列表") List<String> controllers) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getControllerConfig(controllers));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "选举主节点")
    public String electMaster(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                              @ToolParam(description = "access key or ak") String ak,
                              @ToolParam(description = "secret key or sk") String sk,
                              @ToolParam(description = "控制器地址") String controllerAddr,
                              @ToolParam(description = "集群名称") String clusterName,
                              @ToolParam(description = "broker名称") String brokerName,
                              @ToolParam(description = "broker ID") Long brokerId) throws MQClientException {
        return callAdmin(admin -> {
            try {
                Pair<ElectMasterResponseHeader, BrokerMemberGroup> result =
                        admin.electMaster(controllerAddr, clusterName, brokerName, brokerId);
                return JSON.toJSONString(result);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理控制器broker元数据")
    public String cleanControllerBrokerData(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "控制器地址") String controllerAddr,
                                            @ToolParam(description = "集群名称") String clusterName,
                                            @ToolParam(description = "broker名称") String brokerName,
                                            @ToolParam(description = "要清理的broker控制器ID") String brokerControllerIdsToClean,
                                            @ToolParam(description = "是否清理活跃broker") boolean isCleanLivingBroker) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.cleanControllerBrokerData(controllerAddr, clusterName, brokerName,
                        brokerControllerIdsToClean, isCleanLivingBroker);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取同步状态数据")
    public String getInSyncStateData(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                     @ToolParam(description = "access key or ak") String ak,
                                     @ToolParam(description = "secret key or sk") String sk,
                                     @ToolParam(description = "控制器地址") String controllerAddress,
                                     @ToolParam(description = "broker列表") List<String> brokers) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getInSyncStateData(controllerAddress, brokers));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取控制器元数据")
    public String getControllerMetaData(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                        @ToolParam(description = "access key or ak") String ak,
                                        @ToolParam(description = "secret key or sk") String sk,
                                        @ToolParam(description = "控制器地址") String controllerAddr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getControllerMetaData(controllerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新和获取组读权限")
    public String updateAndGetGroupReadForbidden(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "broker地址") String brokerAddr,
                                                 @ToolParam(description = "消费者组") String groupName,
                                                 @ToolParam(description = "主题名称") String topicName,
                                                 @ToolParam(description = "是否可读") Boolean readable) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.updateAndGetGroupReadForbidden(brokerAddr, groupName, topicName, readable));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除主题(按集群)")
    public String deleteTopic(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                              @ToolParam(description = "access key or ak") String ak,
                              @ToolParam(description = "secret key or sk") String sk,
                              @ToolParam(description = "主题名称") String topicName,
                              @ToolParam(description = "集群名称") String clusterName) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.deleteTopic(topicName, clusterName);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除主题(在NameServer中)")
    public String deleteTopicInNameServer(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "nameserver地址列表") Set<String> addrs,
                                          @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.deleteTopicInNameServer(addrs, topic);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除主题(在NameServer中带集群)")
    public String deleteTopicInNameServerWithCluster(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                     @ToolParam(description = "access key or ak") String ak,
                                                     @ToolParam(description = "secret key or sk") String sk,
                                                     @ToolParam(description = "nameserver地址列表") Set<String> addrs,
                                                     @ToolParam(description = "集群名称") String clusterName,
                                                     @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.deleteTopicInNameServer(addrs, clusterName, topic);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "重置消费者偏移量(新版)")
    public String resetOffsetNew(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                 @ToolParam(description = "access key or ak") String ak,
                                 @ToolParam(description = "secret key or sk") String sk,
                                 @ToolParam(description = "消费者组") String consumerGroup,
                                 @ToolParam(description = "主题") String topic,
                                 @ToolParam(description = "时间戳") long timestamp) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.resetOffsetNew(consumerGroup, topic, timestamp);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消费状态")
    public String getConsumeStatus(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "主题") String topic,
                                   @ToolParam(description = "消费者组") String group,
                                   @ToolParam(description = "客户端地址") String clientAddr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getConsumeStatus(topic, group, clientAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "克隆消费者组偏移量")
    public String cloneGroupOffset(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "源消费者组") String srcGroup,
                                   @ToolParam(description = "目标消费者组") String destGroup,
                                   @ToolParam(description = "主题") String topic,
                                   @ToolParam(description = "是否离线") boolean isOffline) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.cloneGroupOffset(srcGroup, destGroup, topic, isOffline);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查看Broker统计信息")
    public String viewBrokerStatsData(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "broker地址") String brokerAddr,
                                      @ToolParam(description = "统计名称") String statsName,
                                      @ToolParam(description = "统计键") String statsKey) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.viewBrokerStatsData(brokerAddr, statsName, statsKey));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取集群列表")
    public String getClusterList(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                 @ToolParam(description = "access key or ak") String ak,
                                 @ToolParam(description = "secret key or sk") String sk,
                                 @ToolParam(description = "主题") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getClusterList(topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker消费统计")
    public String fetchConsumeStatsInBroker(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "broker地址") String brokerAddr,
                                            @ToolParam(description = "是否顺序消费") boolean isOrder,
                                            @ToolParam(description = "超时时间") long timeoutMillis) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.fetchConsumeStatsInBroker(brokerAddr, isOrder, timeoutMillis));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取主题集群列表")
    public String getTopicClusterList(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "主题") String topic) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getTopicClusterList(topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取用户订阅组")
    public String getUserSubscriptionGroup(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                           @ToolParam(description = "access key or ak") String ak,
                                           @ToolParam(description = "secret key or sk") String sk,
                                           @ToolParam(description = "broker地址") String brokerAddr,
                                           @ToolParam(description = "超时时间") long timeoutMillis) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getUserSubscriptionGroup(brokerAddr, timeoutMillis));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取用户主题配置")
    public String getUserTopicConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                     @ToolParam(description = "access key or ak") String ak,
                                     @ToolParam(description = "secret key or sk") String sk,
                                     @ToolParam(description = "broker地址") String brokerAddr,
                                     @ToolParam(description = "是否特殊主题") boolean specialTopic,
                                     @ToolParam(description = "超时时间") long timeoutMillis) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getUserTopicConfig(brokerAddr, specialTopic, timeoutMillis));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新NameServer配置")
    public String updateNameServerConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "配置属性") Properties properties,
                                         @ToolParam(description = "nameserver地址列表") List<String> nameServers) throws MQClientException {
        return callAdmin(admin -> {
            try {
                admin.updateNameServerConfig(properties, nameServers);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取NameServer配置")
    public String getNameServerConfig(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "nameserver地址列表") List<String> nameServers) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getNameServerConfig(nameServers));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除过期提交日志")
    public String deleteExpiredCommitLog(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "集群名称") String cluster) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return String.valueOf(admin.deleteExpiredCommitLog(cluster));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除过期提交日志(按地址)")
    public String deleteExpiredCommitLogByAddr(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                               @ToolParam(description = "access key or ak") String ak,
                                               @ToolParam(description = "secret key or sk") String sk,
                                               @ToolParam(description = "broker地址") String addr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return String.valueOf(admin.deleteExpiredCommitLogByAddr(addr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理过期消费队列")
    public String cleanExpiredConsumerQueue(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "集群名称") String cluster) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return String.valueOf(admin.cleanExpiredConsumerQueue(cluster));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理过期消费队列(按地址)")
    public String cleanExpiredConsumerQueueByAddr(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                                  @ToolParam(description = "access key or ak") String ak,
                                                  @ToolParam(description = "secret key or sk") String sk,
                                                  @ToolParam(description = "broker地址") String addr) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return String.valueOf(admin.cleanExpiredConsumerQueueByAddr(addr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消息轨迹详情")
    public String messageTrackDetail(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                     @ToolParam(description = "access key or ak") String ak,
                                     @ToolParam(description = "secret key or sk") String sk,
                                     @ToolParam(description = "消息") String messageJson) throws MQClientException {
        return callAdmin(admin -> {
            try {
                MessageExt msg = JSON.parseObject(messageJson, MessageExt.class);
                return JSON.toJSONString(admin.messageTrackDetail(msg));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消息轨迹详情(并发)")
    public String messageTrackDetailConcurrent(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                               @ToolParam(description = "access key or ak") String ak,
                                               @ToolParam(description = "secret key or sk") String sk,
                                               @ToolParam(description = "消息") String messageJson) throws MQClientException {
        return callAdmin(admin -> {
            try {
                MessageExt msg = JSON.parseObject(messageJson, MessageExt.class);
                return JSON.toJSONString(admin.messageTrackDetailConcurrent(msg));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新消费偏移量")
    public String updateConsumeOffset(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "broker地址") String brokerAddr,
                                      @ToolParam(description = "消费者组") String consumeGroup,
                                      @ToolParam(description = "消息队列") String mqJson,
                                      @ToolParam(description = "偏移量") long offset) throws MQClientException {
        return callAdmin(admin -> {
            try {
                MessageQueue mq = JSON.parseObject(mqJson, MessageQueue.class);
                admin.updateConsumeOffset(brokerAddr, consumeGroup, mq, offset);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "搜索偏移量")
    public String searchOffset(@ToolParam(description = "nameserver 地址列表") String nameserverAddressList,
                               @ToolParam(description = "access key or ak") String ak,
                               @ToolParam(description = "secret key or sk") String sk,
                               @ToolParam(description = "broker地址") String brokerAddr,
                               @ToolParam(description = "主题名称") String topicName,
                               @ToolParam(description = "队列ID") int queueId,
                               @ToolParam(description = "时间戳") long timestamp,
                               @ToolParam(description = "超时时间") long timeoutMillis) throws MQClientException {
        return callAdmin(admin -> {
            try {
                return String.valueOf(admin.searchOffset(brokerAddr, topicName, queueId, timestamp, timeoutMillis));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}
