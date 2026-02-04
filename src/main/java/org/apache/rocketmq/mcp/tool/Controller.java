package org.apache.rocketmq.mcp.tool;

import com.alibaba.fastjson2.JSON;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class Controller {

    @Tool(description = "更新控制器配置")
    public String updateControllerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "配置属性") Properties properties,
                                         @ToolParam(description = "控制器地址列表") List<String> controllers) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.updateControllerConfig(properties, controllers);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取控制器配置")
    public String getControllerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "控制器地址列表") List<String> controllers) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getControllerConfig(controllers));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "选举主节点")
    public String electMaster(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                              @ToolParam(description = "access key or ak") String ak,
                              @ToolParam(description = "secret key or sk") String sk,
                              @ToolParam(description = "控制器地址") String controllerAddr,
                              @ToolParam(description = "集群名称") String clusterName,
                              @ToolParam(description = "broker名称") String brokerName,
                              @ToolParam(description = "broker ID") Long brokerId) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
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
    public String cleanControllerBrokerData(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "控制器地址") String controllerAddr,
                                            @ToolParam(description = "集群名称") String clusterName,
                                            @ToolParam(description = "broker名称") String brokerName,
                                            @ToolParam(description = "要清理的broker控制器ID") String brokerControllerIdsToClean,
                                            @ToolParam(description = "是否清理活跃broker") boolean isCleanLivingBroker) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
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
    public String getInSyncStateData(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                     @ToolParam(description = "access key or ak") String ak,
                                     @ToolParam(description = "secret key or sk") String sk,
                                     @ToolParam(description = "控制器地址") String controllerAddress,
                                     @ToolParam(description = "broker列表") List<String> brokers) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getInSyncStateData(controllerAddress, brokers));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取控制器元数据")
    public String getControllerMetaData(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                        @ToolParam(description = "access key or ak") String ak,
                                        @ToolParam(description = "secret key or sk") String sk,
                                        @ToolParam(description = "控制器地址") String controllerAddr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getControllerMetaData(controllerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}
