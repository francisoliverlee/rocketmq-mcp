package org.apache.rocketmq.mcp;

import org.apache.rocketmq.mcp.tool.*;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

@Service
public class ToolsLoader {
    @Bean
    public ToolCallbackProvider buildAclTool(Acl tool) {
        return MethodToolCallbackProvider.builder().toolObjects(tool).build();
    }
    @Bean
    public ToolCallbackProvider buildBrokerTool(Broker tool) {
        return MethodToolCallbackProvider.builder().toolObjects(tool).build();
    }

    @Bean
    public ToolCallbackProvider buildClusterTool(Cluster tool) {
        return MethodToolCallbackProvider.builder().toolObjects(tool).build();
    }

    @Bean
    public ToolCallbackProvider buildConsumeQueueTool(ConsumeQueue tool) {
        return MethodToolCallbackProvider.builder().toolObjects(tool).build();
    }

    @Bean
    public ToolCallbackProvider buildConsumerTool(Consumer tool) {
        return MethodToolCallbackProvider.builder().toolObjects(tool).build();
    }

    @Bean
    public ToolCallbackProvider buildControllerTool(Controller tool) {
        return MethodToolCallbackProvider.builder().toolObjects(tool).build();
    }

    @Bean
    public ToolCallbackProvider buildMessageTool(Message tool) {
        return MethodToolCallbackProvider.builder().toolObjects(tool).build();
    }

    @Bean
    public ToolCallbackProvider buildNameserverTool(Nameserver tool) {
        return MethodToolCallbackProvider.builder().toolObjects(tool).build();
    }

    @Bean
    public ToolCallbackProvider buildProducerTool(Producer tool) {
        return MethodToolCallbackProvider.builder().toolObjects(tool).build();
    }

    @Bean
    public ToolCallbackProvider buildTopicTool(Topic tool) {
        return MethodToolCallbackProvider.builder().toolObjects(tool).build();
    }
}
