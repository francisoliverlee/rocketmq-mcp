package org.apache.rocketmq.mcp.tool;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

import java.util.List;
import java.util.Properties;

@org.springframework.stereotype.Service
public class Nameserver {

    @Tool(description = "获取NameServer地址列表")
    public String getNameServerAddressList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                           @ToolParam(description = "access key or ak") String ak,
                                           @ToolParam(description = "secret key or sk") String sk) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getNameServerAddressList());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "添加KV配置")
    public String putKVConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                              @ToolParam(description = "access key or ak") String ak,
                              @ToolParam(description = "secret key or sk") String sk,
                              @ToolParam(description = "命名空间") String namespace,
                              @ToolParam(description = "键") String key,
                              @ToolParam(description = "值") String value) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.putKVConfig(namespace, key, value);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取KV配置")
    public String getKVConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                              @ToolParam(description = "access key or ak") String ak,
                              @ToolParam(description = "secret key or sk") String sk,
                              @ToolParam(description = "命名空间") String namespace,
                              @ToolParam(description = "键") String key) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return admin.getKVConfig(namespace, key);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取命名空间下的KV列表")
    public String getKVListByNamespace(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "命名空间") String namespace) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getKVListByNamespace(namespace));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建或更新KV配置")
    public String createAndUpdateKvConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "命名空间") String namespace,
                                          @ToolParam(description = "键") String key,
                                          @ToolParam(description = "值") String value) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.createAndUpdateKvConfig(namespace, key, value);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除KV配置")
    public String deleteKvConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                 @ToolParam(description = "access key or ak") String ak,
                                 @ToolParam(description = "secret key or sk") String sk,
                                 @ToolParam(description = "命名空间") String namespace,
                                 @ToolParam(description = "键") String key) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.deleteKvConfig(namespace, key);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建或更新顺序配置")
    public String createOrUpdateOrderConf(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "键") String key,
                                          @ToolParam(description = "值") String value,
                                          @ToolParam(description = "是否集群") boolean isCluster) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.createOrUpdateOrderConf(key, value, isCluster);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新NameServer配置")
    public String updateNameServerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "配置属性") Properties properties,
                                         @ToolParam(description = "nameserver地址列表") List<String> nameServers) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.updateNameServerConfig(properties, nameServers);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取NameServer配置")
    public String getNameServerConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "nameserver地址列表") List<String> nameServers) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getNameServerConfig(nameServers));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}
