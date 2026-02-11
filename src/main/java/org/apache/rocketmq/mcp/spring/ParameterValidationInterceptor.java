package org.apache.rocketmq.mcp.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * 参数验证拦截器
 * 用于验证所有带有@ToolParam注解的参数不能为空
 * <p>
 * 注意：Spring AI的工具调用是通过特定的机制进行的，参数验证主要通过以下方式：
 * 1. 在工具方法内部进行参数验证
 * 2. 使用统一的参数验证工具类
 * 3. 在AdminUtil.callAdminWithResponse方法中统一验证
 */
@Component
public class ParameterValidationInterceptor implements HandlerInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(ParameterValidationInterceptor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 只处理HandlerMethod类型的handler
        if (!(handler instanceof HandlerMethod)) {
            return true;
        }

        HandlerMethod handlerMethod = (HandlerMethod) handler;
        Method method = handlerMethod.getMethod();

        // 检查方法是否有@ToolParam注解的参数
        if (!hasToolParamAnnotation(method)) {
            return true;
        }

        // 对于Spring AI工具调用，参数验证主要在工具方法内部进行
        // 这里我们主要记录日志，不进行实际的拦截验证
        logger.debug("检测到带有@ToolParam注解的方法: {}", method.getName());

        // 记录参数信息用于调试
        Parameter[] parameters = method.getParameters();
        for (Parameter parameter : parameters) {
            ToolParam toolParam = parameter.getAnnotation(ToolParam.class);
            if (toolParam != null) {
                logger.debug("参数验证: {} - {}", parameter.getName(), toolParam.description());
            }
        }

        return true;
    }

    /**
     * 检查方法是否包含@ToolParam注解
     */
    private boolean hasToolParamAnnotation(Method method) {
        Parameter[] parameters = method.getParameters();
        for (Parameter parameter : parameters) {
            if (parameter.isAnnotationPresent(ToolParam.class)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 检查参数是否为空
     */
    private boolean isParameterEmpty(Object paramValue, Class<?> paramType) {
        if (paramValue == null) {
            return true;
        }

        if (paramValue instanceof String) {
            return StringUtils.isBlank((String) paramValue);
        }

        if (paramValue instanceof List) {
            return ((List<?>) paramValue).isEmpty();
        }

        if (paramValue.getClass().isArray()) {
            return ((Object[]) paramValue).length == 0;
        }

        return false;
    }
}