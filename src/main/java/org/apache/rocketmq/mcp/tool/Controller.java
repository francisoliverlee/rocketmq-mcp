package org.apache.rocketmq.mcp.tool;

import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.mcp.common.ApiResponse;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class Controller {
    public static final List<String> WRITE_OPERATIONS = List.of(
            "updateControllerConfig",
            "electMaster",
            "cleanExpiredController",
            "cleanExpiredControllerByAddr",
            "cleanControllerBrokerData"
    );

    @Tool(description = "更新控制器配置")
    public ApiResponse<String> updateControllerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                      @ToolParam(description = "access key or ak") String ak,
                                                      @ToolParam(description = "secret key or sk") String sk,
                                                      @ToolParam(description = "配置属性") Properties properties,
                                                      @ToolParam(description = "控制器地址列表") List<String> controllers) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.updateControllerConfig(properties, controllers);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取控制器配置")
    public ApiResponse<Object> getControllerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "控制器地址列表") List<String> controllers) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getControllerConfig(controllers);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "选举主节点")
    public ApiResponse<Pair<ElectMasterResponseHeader, BrokerMemberGroup>> electMaster(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                                       @ToolParam(description = "access key or ak") String ak,
                                                                                       @ToolParam(description = "secret key or sk") String sk,
                                                                                       @ToolParam(description = "控制器地址") String controllerAddr,
                                                                                       @ToolParam(description = "broker名称") String brokerName,
                                                                                       @ToolParam(description = "集群名称") String clusterName,
                                                                                       @ToolParam(description = "broker id") Long brokerId) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.electMaster(controllerAddr, brokerName, clusterName, brokerId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理过期控制器")
    public ApiResponse<String> cleanExpiredController(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                      @ToolParam(description = "access key or ak") String ak,
                                                      @ToolParam(description = "secret key or sk") String sk,
                                                      @ToolParam(description = "controller 地址列表") String controllerAddr,
                                                      @ToolParam(description = "集群名称") String clusterName,
                                                      @ToolParam(description = "broker 名字") String brokerName,
                                                      @ToolParam(description = "要清理的broker控制器ID") String brokerControllerIdsToClean,
                                                      @ToolParam(description = "是否清理活跃broker") boolean isCleanLivingBroker) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.cleanControllerBrokerData(controllerAddr, clusterName, brokerName, brokerControllerIdsToClean, isCleanLivingBroker);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理控制器broker元数据")
    public ApiResponse<String> cleanControllerBrokerData(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                         @ToolParam(description = "access key or ak") String ak,
                                                         @ToolParam(description = "secret key or sk") String sk,
                                                         @ToolParam(description = "控制器地址") String controllerAddr,
                                                         @ToolParam(description = "集群名称") String clusterName,
                                                         @ToolParam(description = "broker名称") String brokerName,
                                                         @ToolParam(description = "要清理的broker控制器ID") String brokerControllerIdsToClean,
                                                         @ToolParam(description = "是否清理活跃broker") boolean isCleanLivingBroker) throws MQClientException {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.cleanControllerBrokerData(controllerAddr, clusterName, brokerName, brokerControllerIdsToClean, isCleanLivingBroker);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取同步状态数据")
    public ApiResponse<Object> getInSyncStateData(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                  @ToolParam(description = "access key or ak") String ak,
                                                  @ToolParam(description = "secret key or sk") String sk,
                                                  @ToolParam(description = "控制器地址") String controllerAddress,
                                                  @ToolParam(description = "broker列表") List<String> brokers) throws MQClientException {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getInSyncStateData(controllerAddress, brokers);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取控制器元数据")
    public ApiResponse<Object> getControllerMetaData(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                     @ToolParam(description = "access key or ak") String ak,
                                                     @ToolParam(description = "secret key or sk") String sk,
                                                     @ToolParam(description = "控制器地址") String controllerAddr) throws MQClientException {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getControllerMetaData(controllerAddr);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}