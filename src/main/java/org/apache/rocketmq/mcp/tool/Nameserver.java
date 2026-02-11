package org.apache.rocketmq.mcp.tool;

import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.mcp.common.ApiResponse;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class Nameserver {
    public static final List<String> WRITE_OPERATIONS = List.of(
            "putKVConfig",
            "createAndUpdateKVConfig",
            "deleteKVConfig",
            "createOrUpdateOrderConf",
            "updateNameServerConfig"
    );

    @Tool(description = "获取NameServer地址列表")
    public ApiResponse<Object> getNameServerAddressList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                        @ToolParam(description = "access key or ak") String ak,
                                                        @ToolParam(description = "secret key or sk") String sk) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getNameServerAddressList();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "添加KV配置")
    public ApiResponse<String> putKVConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                           @ToolParam(description = "access key or ak") String ak,
                                           @ToolParam(description = "secret key or sk") String sk,
                                           @ToolParam(description = "命名空间") String namespace,
                                           @ToolParam(description = "键") String key,
                                           @ToolParam(description = "值") String value) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.putKVConfig(namespace, key, value);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取KV配置")
    public ApiResponse<Object> getKVConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                           @ToolParam(description = "access key or ak") String ak,
                                           @ToolParam(description = "secret key or sk") String sk,
                                           @ToolParam(description = "命名空间") String namespace,
                                           @ToolParam(description = "键") String key) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getKVConfig(namespace, key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取命名空间下的KV列表")
    public ApiResponse<Object> getKVListByNamespace(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                    @ToolParam(description = "access key or ak") String ak,
                                                    @ToolParam(description = "secret key or sk") String sk,
                                                    @ToolParam(description = "命名空间") String namespace) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getKVListByNamespace(namespace);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建或更新KV配置")
    public ApiResponse<String> createAndUpdateKVConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                       @ToolParam(description = "access key or ak") String ak,
                                                       @ToolParam(description = "secret key or sk") String sk,
                                                       @ToolParam(description = "命名空间") String namespace,
                                                       @ToolParam(description = "键") String key,
                                                       @ToolParam(description = "值") String value) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.createAndUpdateKvConfig(namespace, key, value);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除KV配置")
    public ApiResponse<String> deleteKVConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                              @ToolParam(description = "access key or ak") String ak,
                                              @ToolParam(description = "secret key or sk") String sk,
                                              @ToolParam(description = "命名空间") String namespace,
                                              @ToolParam(description = "键") String key) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.deleteKvConfig(namespace, key);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取KV配置列表")
    public ApiResponse<Object> getKVConfigList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                               @ToolParam(description = "access key or ak") String ak,
                                               @ToolParam(description = "secret key or sk") String sk,
                                               @ToolParam(description = "命名空间") String namespace) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getKVListByNamespace(namespace);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取KV配置值")
    public ApiResponse<Object> getKVConfigValue(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "命名空间") String namespace,
                                                @ToolParam(description = "键") String key) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getKVConfig(namespace, key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取KV配置值列表")
    public ApiResponse<Object> getKVConfigValueList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                    @ToolParam(description = "access key or ak") String ak,
                                                    @ToolParam(description = "secret key or sk") String sk,
                                                    @ToolParam(description = "命名空间") String namespace) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getKVListByNamespace(namespace);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建或更新顺序配置")
    public ApiResponse<String> createOrUpdateOrderConf(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                       @ToolParam(description = "access key or ak") String ak,
                                                       @ToolParam(description = "secret key or sk") String sk,
                                                       @ToolParam(description = "键") String key,
                                                       @ToolParam(description = "值") String value,
                                                       @ToolParam(description = "是否集群") boolean isCluster) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.createOrUpdateOrderConf(key, value, isCluster);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return "success";
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新NameServer配置")
    public ApiResponse<String> updateNameServerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                      @ToolParam(description = "access key or ak") String ak,
                                                      @ToolParam(description = "secret key or sk") String sk,
                                                      @ToolParam(description = "配置属性") Properties properties,
                                                      @ToolParam(description = "nameserver地址列表") List<String> nameServers) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.updateNameServerConfig(properties, nameServers);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return "success";
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取NameServer配置")
    public ApiResponse<Object> getNameServerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "nameserver地址列表") List<String> nameServers) throws MQClientException {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getNameServerConfig(nameServers);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}