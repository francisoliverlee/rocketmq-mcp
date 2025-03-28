package org.apache.rocketmq.mcp;

import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class Startup {
    public static void main(String[] args) {
        SpringApplication.run(Startup.class, args);
    }

    @Bean
    public ToolCallbackProvider buildClusterService(Service clusterService) {
        return MethodToolCallbackProvider.builder().toolObjects(clusterService).build();
    }
}
