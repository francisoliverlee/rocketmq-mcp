package org.apache.rocketmq.mcp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"org.apache.rocketmq.mcp", "org.springframework.ai.mcp"})
public class Startup {
    public static void main(String[] args) {
        SpringApplication.run(Startup.class, args);
    }
}