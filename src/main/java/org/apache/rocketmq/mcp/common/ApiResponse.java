package org.apache.rocketmq.mcp.common;

import com.alibaba.fastjson2.JSON;

/**
 * 统一的API响应格式
 */
public class ApiResponse<T> {

    /**
     * 错误码：0表示成功，非0表示失败
     */
    private int errorCode;

    /**
     * 错误信息：成功时为null或空字符串，失败时为错误描述
     */
    private String errorMessage;

    /**
     * 业务数据：实际返回的业务信息
     */
    private T data;

    public ApiResponse() {
    }

    public ApiResponse(int errorCode, String errorMessage, T data) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.data = data;
    }

    /**
     * 创建成功响应
     */
    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<T>(0, null, data);
    }

    /**
     * 创建成功响应（无数据）
     */
    public static <T> ApiResponse<T> success() {
        return success(null);
    }

    /**
     * 创建失败响应
     */
    public static <T> ApiResponse<T> error(int errorCode, String errorMessage) {
        return new ApiResponse<T>(errorCode, errorMessage, null);
    }

    /**
     * 创建失败响应（默认错误码）
     */
    public static <T> ApiResponse<T> error(String errorMessage) {
        return error(-1, errorMessage);
    }

    /**
     * 转换为JSON字符串
     */
    public String toJsonString() {
        return JSON.toJSONString(this);
    }

    // Getter和Setter方法
    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return toJsonString();
    }
}