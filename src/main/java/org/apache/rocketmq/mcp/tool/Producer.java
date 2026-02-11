package org.apache.rocketmq.mcp.tool;

import java.util.List;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.mcp.common.ApiResponse;
import org.apache.rocketmq.remoting.protocol.body.ProducerConnection;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class Producer {
    public static final List<String> WRITE_OPERATIONS = List.of(

    );

    @Tool(description = "获取生产者连接信息")
    public ApiResponse<ProducerConnection> examineProducerConnectionInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                         @ToolParam(description = "access key or ak") String ak,
                                                                         @ToolParam(description = "secret key or sk") String sk,
                                                                         @ToolParam(description = "生产者组名称") String producerGroup,
                                                                         @ToolParam(description = "主题名称") String topic) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineProducerConnectionInfo(producerGroup, topic);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取所有生产者信息")
    public ApiResponse<ProducerTableInfo> getAllProducerInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                             @ToolParam(description = "access key or ak") String ak,
                                                             @ToolParam(description = "secret key or sk") String sk,
                                                             @ToolParam(description = "broker地址") String brokerAddr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getAllProducerInfo(brokerAddr);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

}