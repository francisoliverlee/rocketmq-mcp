package org.apache.rocketmq.mcp.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Autowired
    private ReadOnlyInterceptor readOnlyInterceptor;

    @Autowired
    private RequestLoggingInterceptor requestLoggingInterceptor;

    @Autowired
    private ParameterValidationInterceptor parameterValidationInterceptor;

    @Bean
    public CorsFilter corsFilter() {
        CorsConfiguration config = new CorsConfiguration();

        // 允许所有域名访问
        config.addAllowedOriginPattern("*");

        // 允许携带凭证
        config.setAllowCredentials(true);

        // 允许所有请求头
        config.addAllowedHeader("*");

        // 允许所有请求方法
        config.addAllowedMethod("*");

        // 设置预检请求的有效期，单位秒
        config.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();

        // 对所有路径应用CORS配置
        source.registerCorsConfiguration("/**", config);

        return new CorsFilter(source);
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 注册参数验证拦截器，拦截所有路径（最先执行）
        registry.addInterceptor(parameterValidationInterceptor)
                .addPathPatterns("/**");

        // 注册请求日志拦截器，拦截所有路径
        registry.addInterceptor(requestLoggingInterceptor)
                .addPathPatterns("/**");

        // 注册只读模式拦截器，拦截所有路径
        registry.addInterceptor(readOnlyInterceptor)
                .addPathPatterns("/**");
    }
}