package org.apache.rocketmq.mcp.tool;

import java.util.List;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.mcp.common.ApiResponse;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class Message {
    public static final List<String> WRITE_OPERATIONS = List.of(
            "consumeMessageDirectly",
            "cleanExpiredMessages",
            "resumeCheckHalfMessage"
    );

    @Tool(description = "直接消费消息")
    public ApiResponse<ConsumeMessageDirectlyResult> consumeMessageDirectly(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                            @ToolParam(description = "access key or ak") String ak,
                                                                            @ToolParam(description = "secret key or sk") String sk,
                                                                            @ToolParam(description = "消费者组") String consumerGroup,
                                                                            @ToolParam(description = "topic") String topic,
                                                                            @ToolParam(description = "客户端ID") String clientId,
                                                                            @ToolParam(description = "消息ID") String msgId) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.consumeMessageDirectly(consumerGroup, clientId, topic, msgId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消息")
    public ApiResponse<MessageExt> viewMessage(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                               @ToolParam(description = "access key or ak") String ak,
                                               @ToolParam(description = "secret key or sk") String sk,
                                               @ToolParam(description = "主题名称") String topic,
                                               @ToolParam(description = "消息ID") String msgId) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.viewMessage(topic, msgId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理过期消息")
    public ApiResponse<String> cleanExpiredMessages(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                    @ToolParam(description = "access key or ak") String ak,
                                                    @ToolParam(description = "secret key or sk") String sk,
                                                    @ToolParam(description = "broker地址") String brokerAddr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.cleanExpiredConsumerQueue(brokerAddr);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "按照消息key查询消息")
    public ApiResponse<QueryResult> queryMessageByKey(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                      @ToolParam(description = "access key or ak") String ak,
                                                      @ToolParam(description = "secret key or sk") String sk,
                                                      @ToolParam(description = "主题") String topic,
                                                      @ToolParam(description = "消息key") String key,
                                                      @ToolParam(description = "最大消息数") int maxNum,
                                                      @ToolParam(description = "开始时间") long begin,
                                                      @ToolParam(description = "结束时间") long end) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.queryMessage(topic, key, maxNum, begin, end);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "按照时间范围查询指定条数 指定broker地址的消息")
    public ApiResponse<QueryResult> queryMessageByKeyAndBroker(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                               @ToolParam(description = "access key or ak") String ak,
                                                               @ToolParam(description = "secret key or sk") String sk,
                                                               @ToolParam(description = "主题") String topic,
                                                               @ToolParam(description = "键") String key,
                                                               @ToolParam(description = "最大消息数") int maxNum,
                                                               @ToolParam(description = "开始时间") long begin,
                                                               @ToolParam(description = "结束时间") long end,
                                                               @ToolParam(description = "broker地址") String brokerAddr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.queryMessage(brokerAddr, topic, key, maxNum, begin, end);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "按照消息id查询消息")
    public ApiResponse<MessageExt> queryMessageById(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                    @ToolParam(description = "access key or ak") String ak,
                                                    @ToolParam(description = "secret key or sk") String sk,
                                                    @ToolParam(description = "主题") String topic,
                                                    @ToolParam(description = "消息ID") String msgId) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.viewMessage(topic, msgId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "按照时间范围查询指定条数的消息")
    public ApiResponse<QueryResult> queryRecentMessages(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                        @ToolParam(description = "access key or ak") String ak,
                                                        @ToolParam(description = "secret key or sk") String sk,
                                                        @ToolParam(description = "主题") String topic,
                                                        @ToolParam(description = "消息Key") String key,
                                                        @ToolParam(description = "最大消息数") int maxNum,
                                                        @ToolParam(description = "开始时间") long begin,
                                                        @ToolParam(description = "结束时间") long end) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.queryMessage(topic, key, maxNum, begin, end);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "恢复检查半消息")
    public ApiResponse<Boolean> resumeCheckHalfMessage(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                       @ToolParam(description = "access key or ak") String ak,
                                                       @ToolParam(description = "secret key or sk") String sk,
                                                       @ToolParam(description = "主题") String topic,
                                                       @ToolParam(description = "消息ID") String msgId) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.resumeCheckHalfMessage(topic, msgId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消息轨迹详情")
    public ApiResponse<List<MessageTrack>> messageTrackDetail(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                              @ToolParam(description = "access key or ak") String ak,
                                                              @ToolParam(description = "secret key or sk") String sk,
                                                              @ToolParam(description = "消息") String messageJson) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                MessageExt msg = com.alibaba.fastjson2.JSON.parseObject(messageJson, MessageExt.class);
                return admin.messageTrackDetail(msg);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消息轨迹详情(并发)")
    public ApiResponse<List<MessageTrack>> messageTrackDetailConcurrent(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                        @ToolParam(description = "access key or ak") String ak,
                                                                        @ToolParam(description = "secret key or sk") String sk,
                                                                        @ToolParam(description = "消息") String messageJson) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                MessageExt msg = com.alibaba.fastjson2.JSON.parseObject(messageJson, MessageExt.class);
                return admin.messageTrackDetailConcurrent(msg);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

}