package org.apache.rocketmq.mcp.tool;

import com.alibaba.fastjson2.JSON;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class Broker {
    public static List<String> getAllBrokerAddresses(DefaultMQAdminExt admin) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        List<String> result = new ArrayList<>();
        ClusterInfo clusterInfo = admin.examineBrokerClusterInfo();
        clusterInfo.getBrokerAddrTable().values().stream().forEach(brokerData -> {
            brokerData.getBrokerAddrs().values().stream().forEach(brokerAddr -> {
                result.add(brokerAddr);
            });
        });
        return result;
    }

    @Tool(description = "获取Broker统计信息")
    public String getBrokerRuntimeStats(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                        @ToolParam(description = "access key or ak") String ak,
                                        @ToolParam(description = "secret key or sk") String sk,
                                        @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.fetchBrokerRuntimeStats(brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker配置信息")
    public String getBrokerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                  @ToolParam(description = "access key or ak") String ak,
                                  @ToolParam(description = "secret key or sk") String sk,
                                  @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getBrokerConfig(brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新Broker配置")
    public String updateBrokerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                     @ToolParam(description = "access key or ak") String ak,
                                     @ToolParam(description = "secret key or sk") String sk,
                                     @ToolParam(description = "broker地址") String brokerAddr,
                                     @ToolParam(description = "配置项") String key,
                                     @ToolParam(description = "配置值") String value) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
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
    public String getAllBrokerAddresses(@ToolParam(description = "nameserver / namesrv 地址列表") List<String> nameserverAddressList,
                                        @ToolParam(description = "access key or ak") String ak,
                                        @ToolParam(description = "secret key or sk") String sk) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(Broker.getAllBrokerAddresses(admin));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "添加broker到容器")
    public String addBrokerToContainer(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "broker容器地址") String brokerContainerAddr,
                                       @ToolParam(description = "broker配置") String brokerConfig) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.addBrokerToContainer(brokerContainerAddr, brokerConfig);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "从容器中移除broker")
    public String removeBrokerFromContainer(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "broker容器地址") String brokerContainerAddr,
                                            @ToolParam(description = "集群名称") String clusterName,
                                            @ToolParam(description = "broker名称") String brokerName,
                                            @ToolParam(description = "broker ID") long brokerId) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.removeBrokerFromContainer(brokerContainerAddr, clusterName, brokerName, brokerId);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "重置主节点刷新偏移量")
    public String resetMasterFlushOffset(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "broker地址") String brokerAddr,
                                         @ToolParam(description = "主节点刷新偏移量") long masterFlushOffset) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.resetMasterFlushOffset(brokerAddr, masterFlushOffset);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker HA状态")
    public String getBrokerHAStatus(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getBrokerHAStatus(brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker Epoch缓存")
    public String getBrokerEpochCache(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getBrokerEpochCache(brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "设置CommitLog预读模式")
    public String setCommitLogReadAheadMode(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "broker地址") String brokerAddr,
                                            @ToolParam(description = "模式") String mode) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return admin.setCommitLogReadAheadMode(brokerAddr, mode);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取冷数据流控制信息")
    public String getColdDataFlowCtrInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return admin.getColdDataFlowCtrInfo(brokerAddr);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新冷数据流控制组配置")
    public String updateColdDataFlowCtrGroupConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "broker地址") String brokerAddr,
                                                   @ToolParam(description = "配置属性") Properties properties) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.updateColdDataFlowCtrGroupConfig(brokerAddr, properties);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "移除冷数据流控制组配置")
    public String removeColdDataFlowCtrGroupConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "broker地址") String brokerAddr,
                                                   @ToolParam(description = "消费者组") String consumerGroup) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.removeColdDataFlowCtrGroupConfig(brokerAddr, consumerGroup);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查看Broker统计信息")
    public String viewBrokerStatsData(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "broker地址") String brokerAddr,
                                      @ToolParam(description = "统计名称") String statsName,
                                      @ToolParam(description = "统计键") String statsKey) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.viewBrokerStatsData(brokerAddr, statsName, statsKey));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除过期提交日志")
    public String deleteExpiredCommitLog(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "集群名称") String cluster) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return String.valueOf(admin.deleteExpiredCommitLog(cluster));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除过期提交日志(按地址)")
    public String deleteExpiredCommitLogByAddr(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                               @ToolParam(description = "access key or ak") String ak,
                                               @ToolParam(description = "secret key or sk") String sk,
                                               @ToolParam(description = "broker地址") String addr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return String.valueOf(admin.deleteExpiredCommitLogByAddr(addr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "搜索偏移量")
    public String searchOffset(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                               @ToolParam(description = "access key or ak") String ak,
                               @ToolParam(description = "secret key or sk") String sk,
                               @ToolParam(description = "broker地址") String brokerAddr,
                               @ToolParam(description = "主题名称") String topicName,
                               @ToolParam(description = "队列ID") int queueId,
                               @ToolParam(description = "时间戳") long timestamp,
                               @ToolParam(description = "超时时间") long timeoutMillis) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return String.valueOf(admin.searchOffset(brokerAddr, topicName, queueId, timestamp, timeoutMillis));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}
