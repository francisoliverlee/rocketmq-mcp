package org.apache.rocketmq.mcp.tool;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

import java.util.List;

/**
 * 利用rocketmq的admin接口提供rocketmq集群信息管理服务
 */
@org.springframework.stereotype.Service
public class Cluster {

    @Tool(description = "获取集群信息")
    public String getClusterInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                 @ToolParam(description = "access key or ak") String ak,
                                 @ToolParam(description = "secret key or sk") String sk) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineBrokerClusterInfo());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }
}
