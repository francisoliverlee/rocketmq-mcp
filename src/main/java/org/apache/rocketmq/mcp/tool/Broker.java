package org.apache.rocketmq.mcp.tool;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.mcp.common.ApiResponse;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class Broker {
    public static final List<String> WRITE_OPERATIONS = List.of(
            "updateBrokerConfig",
            "addBrokerToContainer",
            "removeBrokerFromContainer",
            "resetMasterFlushOffset",
            "setCommitLogReadAheadMode",
            "updateColdDataFlowCtrGroupConfig",
            "removeColdDataFlowCtrGroupConfig",
            "deleteExpiredCommitLog",
            "deleteExpiredCommitLogByAddr"
    );

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
    public ApiResponse<Object> getBrokerRuntimeStats(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                     @ToolParam(description = "access key or ak") String ak,
                                                     @ToolParam(description = "secret key or sk") String sk,
                                                     @ToolParam(description = "broker地址") String brokerAddr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.fetchBrokerRuntimeStats(brokerAddr);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker配置信息")
    public ApiResponse<Object> getBrokerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                               @ToolParam(description = "access key or ak") String ak,
                                               @ToolParam(description = "secret key or sk") String sk,
                                               @ToolParam(description = "broker地址") String brokerAddr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getBrokerConfig(brokerAddr);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新Broker配置")
    public ApiResponse<String> updateBrokerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                  @ToolParam(description = "access key or ak") String ak,
                                                  @ToolParam(description = "secret key or sk") String sk,
                                                  @ToolParam(description = "broker地址") String brokerAddr,
                                                  @ToolParam(description = "配置属性") Properties properties) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.updateBrokerConfig(brokerAddr, properties);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取全部broker地址列表")
    public ApiResponse<List<String>> getAllBrokerAddresses(@ToolParam(description = "nameserver / namesrv 地址列表") List<String> nameserverAddressList,
                                                           @ToolParam(description = "access key or ak") String ak,
                                                           @ToolParam(description = "secret key or sk") String sk) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return Broker.getAllBrokerAddresses(admin);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "添加broker到容器")
    public ApiResponse<String> addBrokerToContainer(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                    @ToolParam(description = "access key or ak") String ak,
                                                    @ToolParam(description = "secret key or sk") String sk,
                                                    @ToolParam(description = "broker容器地址") String brokerContainerAddr,
                                                    @ToolParam(description = "broker配置") String brokerConfig) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.addBrokerToContainer(brokerContainerAddr, brokerConfig);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "从容器中移除broker")
    public ApiResponse<String> removeBrokerFromContainer(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                         @ToolParam(description = "access key or ak") String ak,
                                                         @ToolParam(description = "secret key or sk") String sk,
                                                         @ToolParam(description = "broker容器地址") String brokerContainerAddr,
                                                         @ToolParam(description = "集群名称") String clusterName,
                                                         @ToolParam(description = "broker名称") String brokerName,
                                                         @ToolParam(description = "broker ID") long brokerId) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.removeBrokerFromContainer(brokerContainerAddr, clusterName, brokerName, brokerId);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "重置主节点刷新偏移量")
    public ApiResponse<String> resetMasterFlushOffset(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                      @ToolParam(description = "access key or ak") String ak,
                                                      @ToolParam(description = "secret key or sk") String sk,
                                                      @ToolParam(description = "broker地址") String brokerAddr,
                                                      @ToolParam(description = "主节点刷新偏移量") long masterFlushOffset) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.resetMasterFlushOffset(brokerAddr, masterFlushOffset);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker HA状态")
    public ApiResponse<Object> getBrokerHAStatus(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "broker地址") String brokerAddr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getBrokerHAStatus(brokerAddr);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取Broker Epoch缓存")
    public ApiResponse<Object> getBrokerEpochCache(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "broker地址") String brokerAddr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getBrokerEpochCache(brokerAddr);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "设置CommitLog预读模式")
    public ApiResponse<String> setCommitLogReadAheadMode(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                         @ToolParam(description = "access key or ak") String ak,
                                                         @ToolParam(description = "secret key or sk") String sk,
                                                         @ToolParam(description = "broker地址") String brokerAddr,
                                                         @ToolParam(description = "模式") String mode) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.setCommitLogReadAheadMode(brokerAddr, mode);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取冷数据流控制信息")
    public ApiResponse<Object> getColdDataFlowCtrInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                      @ToolParam(description = "access key or ak") String ak,
                                                      @ToolParam(description = "secret key or sk") String sk,
                                                      @ToolParam(description = "broker地址") String brokerAddr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getColdDataFlowCtrInfo(brokerAddr);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新冷数据流控制组配置")
    public ApiResponse<String> updateColdDataFlowCtrGroupConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                @ToolParam(description = "access key or ak") String ak,
                                                                @ToolParam(description = "secret key or sk") String sk,
                                                                @ToolParam(description = "broker地址") String brokerAddr,
                                                                @ToolParam(description = "配置属性") Properties properties) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.updateColdDataFlowCtrGroupConfig(brokerAddr, properties);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "移除冷数据流控制组配置")
    public ApiResponse<String> removeColdDataFlowCtrGroupConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                @ToolParam(description = "access key or ak") String ak,
                                                                @ToolParam(description = "secret key or sk") String sk,
                                                                @ToolParam(description = "broker地址") String brokerAddr,
                                                                @ToolParam(description = "消费者组") String consumerGroup) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.removeColdDataFlowCtrGroupConfig(brokerAddr, consumerGroup);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查看Broker统计信息")
    public ApiResponse<Object> viewBrokerStatsData(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "broker地址") String brokerAddr,
                                                   @ToolParam(description = "统计名称") String statsName,
                                                   @ToolParam(description = "统计键") String statsKey) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.viewBrokerStatsData(brokerAddr, statsName, statsKey);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除过期提交日志")
    public ApiResponse<String> deleteExpiredCommitLog(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                      @ToolParam(description = "access key or ak") String ak,
                                                      @ToolParam(description = "secret key or sk") String sk,
                                                      @ToolParam(description = "集群名称") String cluster) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.deleteExpiredCommitLog(cluster);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除过期提交日志(按地址)")
    public ApiResponse<String> deleteExpiredCommitLogByAddr(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                            @ToolParam(description = "access key or ak") String ak,
                                                            @ToolParam(description = "secret key or sk") String sk,
                                                            @ToolParam(description = "broker地址") String addr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.deleteExpiredCommitLogByAddr(addr);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "搜索偏移量")
    public ApiResponse<Long> searchOffset(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "broker地址") String brokerAddr,
                                          @ToolParam(description = "主题名称") String topicName,
                                          @ToolParam(description = "队列ID") int queueId,
                                          @ToolParam(description = "时间戳") long timestamp,
                                          @ToolParam(description = "超时时间") long timeoutMillis) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.searchOffset(brokerAddr, topicName, queueId, timestamp, timeoutMillis);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

}