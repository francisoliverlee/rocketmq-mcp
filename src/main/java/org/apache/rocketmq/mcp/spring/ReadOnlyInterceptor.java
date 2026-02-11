package org.apache.rocketmq.mcp.spring;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.mcp.tool.Acl;
import org.apache.rocketmq.mcp.tool.Broker;
import org.apache.rocketmq.mcp.tool.Consumer;
import org.apache.rocketmq.mcp.tool.Controller;
import org.apache.rocketmq.mcp.tool.Message;
import org.apache.rocketmq.mcp.tool.Nameserver;
import org.apache.rocketmq.mcp.tool.Producer;
import org.apache.rocketmq.mcp.tool.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class ReadOnlyInterceptor implements HandlerInterceptor {
    // 统一的写操作方法列表
    public static final List<String> WRITE_OPERATIONS = new ArrayList<>();

    static {
        ReadOnlyInterceptor.WRITE_OPERATIONS.addAll(Nameserver.WRITE_OPERATIONS.stream().map(String::toLowerCase).toList());
        ReadOnlyInterceptor.WRITE_OPERATIONS.addAll(Broker.WRITE_OPERATIONS.stream().map(String::toLowerCase).toList());
        ReadOnlyInterceptor.WRITE_OPERATIONS.addAll(Acl.WRITE_OPERATIONS.stream().map(String::toLowerCase).toList());

        ReadOnlyInterceptor.WRITE_OPERATIONS.addAll(Topic.WRITE_OPERATIONS.stream().map(String::toLowerCase).toList());
        ReadOnlyInterceptor.WRITE_OPERATIONS.addAll(Producer.WRITE_OPERATIONS.stream().map(String::toLowerCase).toList());
        ReadOnlyInterceptor.WRITE_OPERATIONS.addAll(Consumer.WRITE_OPERATIONS.stream().map(String::toLowerCase).toList());
        ReadOnlyInterceptor.WRITE_OPERATIONS.addAll(Message.WRITE_OPERATIONS.stream().map(String::toLowerCase).toList());

        ReadOnlyInterceptor.WRITE_OPERATIONS.addAll(Controller.WRITE_OPERATIONS.stream().map(String::toLowerCase).toList());
    }

    @Autowired
    private McpConfig mcpConfig;


    @Override
    public boolean preHandle(jakarta.servlet.http.HttpServletRequest request, jakarta.servlet.http.HttpServletResponse response, Object handler) throws Exception {
        // 如果不是只读模式，直接放行
        if (!mcpConfig.isReadOnly()) {
            return true;
        }

        // 检查请求路径是否包含写操作方法名
        String requestURI = request.getRequestURI();
        if (containsWriteOperation(requestURI)) {
            // 在只读模式下，拒绝写操作
            response.setStatus(HttpStatus.METHOD_NOT_ALLOWED.value());
            response.getWriter().write("只读模式已启用，不允许执行写操作");
            return false;
        }

        return true;
    }

    /**
     * 检查请求路径是否包含写操作方法名
     */
    private boolean containsWriteOperation(String requestURI) {
        for (String writeOperation : WRITE_OPERATIONS) {
            if (requestURI.toLowerCase().contains(writeOperation)) {
                return true;
            }
        }
        return false;
    }
}