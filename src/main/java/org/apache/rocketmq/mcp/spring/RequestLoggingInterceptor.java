package org.apache.rocketmq.mcp.spring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;

/**
 * 请求日志拦截器
 * 用于打印请求和响应的详细信息，包括URL、方法、请求体和响应体
 */
@Component
public class RequestLoggingInterceptor implements HandlerInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(RequestLoggingInterceptor.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // Jackson ObjectMapper用于JSON格式化
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        // 配置ObjectMapper以美化输出
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 包装请求以支持多次读取请求体
        if (!(request instanceof ContentCachingRequestWrapper)) {
            request = new ContentCachingRequestWrapper(request, 0);
        }

        // 包装响应以支持多次读取响应体
        if (!(response instanceof ContentCachingResponseWrapper)) {
            response = new ContentCachingResponseWrapper(response);
        }

        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 确保请求和响应都被包装
        ContentCachingRequestWrapper wrappedRequest = request instanceof ContentCachingRequestWrapper
                ? (ContentCachingRequestWrapper) request
                : new ContentCachingRequestWrapper(request, 0);

        ContentCachingResponseWrapper wrappedResponse = response instanceof ContentCachingResponseWrapper
                ? (ContentCachingResponseWrapper) response
                : new ContentCachingResponseWrapper(response);

        // 记录请求信息
        logRequest(wrappedRequest);

        // 记录响应信息
        logResponse(wrappedResponse);

        // 复制响应体到原始响应
        wrappedResponse.copyBodyToResponse();
    }

    /**
     * 记录请求信息
     */
    private void logRequest(ContentCachingRequestWrapper request) {
        StringBuilder logBuilder = new StringBuilder();
        logBuilder.append("\n");
        logBuilder.append("┌─────────────────────────────────────────────────────────────────────────────┐\n");
        logBuilder.append("│                             REQUEST LOG                                    │\n");
        logBuilder.append("├─────────────────────────────────────────────────────────────────────────────┤\n");
        logBuilder.append(String.format("│ Time: %-65s │\n", LocalDateTime.now().format(formatter)));
        logBuilder.append(String.format("│ URL: %-65s │\n", truncateString(request.getRequestURL().toString(), 65)));
        logBuilder.append(String.format("│ Method: %-62s │\n", request.getMethod()));
        logBuilder.append(String.format("│ Remote Address: %-56s │\n", request.getRemoteAddr()));
        logBuilder.append(String.format("│ Content-Type: %-59s │\n", request.getContentType()));
        logBuilder.append("├─────────────────────────────────────────────────────────────────────────────┤\n");

        // 记录请求头
        logBuilder.append("│ Headers:\n");
        request.getHeaderNames().asIterator().forEachRemaining(headerName -> {
            String headerValue = request.getHeader(headerName);
            logBuilder.append(String.format("│   %-20s: %-43s │\n",
                    truncateString(headerName, 20), truncateString(headerValue, 43)));
        });

        // 记录请求参数
        if (!request.getParameterMap().isEmpty()) {
            logBuilder.append("│ Parameters:\n");
            request.getParameterMap().forEach((paramName, paramValues) -> {
                String paramValue = String.join(", ", paramValues);
                logBuilder.append(String.format("│   %-20s: %-43s │\n",
                        truncateString(paramName, 20), truncateString(paramValue, 43)));
            });
        }

        // 记录请求体
        byte[] content = request.getContentAsByteArray();
        if (content.length > 0) {
            String requestBody = new String(content, StandardCharsets.UTF_8);
            String formattedBody = formatJsonIfPossible(requestBody);
            logBuilder.append("│ Request Body:\n");

            // 格式化请求体，每行添加边框
            String[] bodyLines = formattedBody.split("\n");
            for (String line : bodyLines) {
                if (line.length() <= 65) {
                    logBuilder.append(String.format("│ %-67s │\n", line));
                } else {
                    // 处理超长行
                    int start = 0;
                    while (start < line.length()) {
                        int end = Math.min(start + 65, line.length());
                        String part = line.substring(start, end);
                        logBuilder.append(String.format("│ %-67s │\n", part));
                        start = end;
                    }
                }
            }
        } else {
            logBuilder.append("│ Request Body: [Empty]\n");
        }

        logBuilder.append("└─────────────────────────────────────────────────────────────────────────────┘\n");

        logger.info(logBuilder.toString());
    }

    /**
     * 截断字符串，如果超过最大长度则添加省略号
     */
    protected String truncateString(String str, int maxLength) {
        if (str == null) {
            return "null";
        }
        if (str.length() <= maxLength) {
            return str;
        }
        return str.substring(0, maxLength - 3) + "...";
    }

    /**
     * 记录响应信息
     */
    private void logResponse(ContentCachingResponseWrapper response) {
        StringBuilder logBuilder = new StringBuilder();
        logBuilder.append("\n");
        logBuilder.append("┌─────────────────────────────────────────────────────────────────────────────┐\n");
        logBuilder.append("│                            RESPONSE LOG                                    │\n");
        logBuilder.append("├─────────────────────────────────────────────────────────────────────────────┤\n");
        logBuilder.append(String.format("│ Time: %-65s │\n", LocalDateTime.now().format(formatter)));
        logBuilder.append(String.format("│ Status: %-62s │\n", response.getStatus()));
        logBuilder.append(String.format("│ Content-Type: %-59s │\n", response.getContentType()));
        logBuilder.append("├─────────────────────────────────────────────────────────────────────────────┤\n");

        // 记录响应头
        if (response.getHeaderNames().size() > 0) {
            logBuilder.append("│ Headers:\n");
            response.getHeaderNames().forEach(headerName -> {
                String headerValue = response.getHeader(headerName);
                logBuilder.append(String.format("│   %-20s: %-43s │\n",
                        truncateString(headerName, 20), truncateString(headerValue, 43)));
            });
        }

        // 记录响应体
        byte[] content = response.getContentAsByteArray();
        if (content.length > 0) {
            String responseBody = new String(content, StandardCharsets.UTF_8);
            String formattedBody = formatJsonIfPossible(responseBody);
            logBuilder.append("│ Response Body:\n");

            // 格式化响应体，每行添加边框
            String[] bodyLines = formattedBody.split("\n");
            for (String line : bodyLines) {
                if (line.length() <= 65) {
                    logBuilder.append(String.format("│ %-67s │\n", line));
                } else {
                    // 处理超长行
                    int start = 0;
                    while (start < line.length()) {
                        int end = Math.min(start + 65, line.length());
                        String part = line.substring(start, end);
                        logBuilder.append(String.format("│ %-67s │\n", part));
                        start = end;
                    }
                }
            }
        } else {
            logBuilder.append("│ Response Body: [Empty]\n");
        }

        logBuilder.append("└─────────────────────────────────────────────────────────────────────────────┘\n");

        logger.info(logBuilder.toString());
    }

    /**
     * 使用Jackson格式化JSON字符串，如果不是JSON则返回原字符串
     */
    protected String formatJsonIfPossible(String content) {
        if (content == null || content.trim().isEmpty()) {
            return content;
        }

        String trimmed = content.trim();

        // 检查是否为JSON格式
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
                (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            try {
                // 先解析JSON以确保格式正确
                Object jsonObject = objectMapper.readValue(trimmed, Object.class);
                // 使用Jackson进行美化格式化
                return objectMapper.writeValueAsString(jsonObject);
            } catch (JsonProcessingException e) {
                // 如果JSON解析失败，可能是格式错误或不是JSON，返回原内容
                logger.debug("JSON格式化失败，返回原内容: {}", e.getMessage());
                return content;
            } catch (Exception e) {
                // 其他异常也返回原内容
                logger.debug("JSON格式化异常: {}", e.getMessage());
                return content;
            }
        }

        // 不是JSON格式，返回原内容
        return content;
    }
}