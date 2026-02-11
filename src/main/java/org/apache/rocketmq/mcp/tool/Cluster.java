package org.apache.rocketmq.mcp.tool;

import java.util.List;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.mcp.common.ApiResponse;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

/**
 * 利用rocketmq的admin接口提供rocketmq集群信息管理服务
 */
@org.springframework.stereotype.Service
public class Cluster {

    @Tool(description = "获取集群信息")
    public ApiResponse<Object> getClusterInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                              @ToolParam(description = "access key or ak") String ak,
                                              @ToolParam(description = "secret key or sk") String sk) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineBrokerClusterInfo();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

}