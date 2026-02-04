package org.apache.rocketmq.mcp.tool;

import com.alibaba.fastjson2.JSON;
import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class Producer {

    @Tool(description = "获取生产者连接信息")
    public String examineProducerConnectionInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                @ToolParam(description = "access key or ak") String ak,
                                                @ToolParam(description = "secret key or sk") String sk,
                                                @ToolParam(description = "生产者组名称") String producerGroup,
                                                @ToolParam(description = "主题名称") String topic) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineProducerConnectionInfo(producerGroup, topic));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取所有生产者信息")
    public String getAllProducerInfo(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                     @ToolParam(description = "access key or ak") String ak,
                                     @ToolParam(description = "secret key or sk") String sk,
                                     @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getAllProducerInfo(brokerAddr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}
