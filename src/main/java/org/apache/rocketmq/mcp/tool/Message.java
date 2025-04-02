package org.apache.rocketmq.mcp.tool;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

import java.util.List;

@org.springframework.stereotype.Service
public class Message {


    @Tool(description = "直接消费消息")
    public String consumeMessageDirectly(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "消费者组") String consumerGroup,
                                         @ToolParam(description = "topic") String topic,
                                         @ToolParam(description = "客户端ID") String clientId,
                                         @ToolParam(description = "消息ID") String msgId) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.consumeMessageDirectly(consumerGroup, clientId, topic, msgId));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "查询消息")
    public String viewMessage(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                              @ToolParam(description = "access key or ak") String ak,
                              @ToolParam(description = "secret key or sk") String sk,
                              @ToolParam(description = "主题名称") String topic,
                              @ToolParam(description = "消息ID") String msgId) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.viewMessage(topic, msgId));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "清理过期消息")
    public String cleanExpiredMessages(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "broker地址") String brokerAddr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.cleanExpiredConsumerQueue(brokerAddr);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消息")
    public String queryMessageByKey(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "主题") String topic,
                                    @ToolParam(description = "键") String key,
                                    @ToolParam(description = "最大消息数") int maxNum,
                                    @ToolParam(description = "开始时间") long begin,
                                    @ToolParam(description = "结束时间") long end) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryMessage(topic, key, maxNum, begin, end));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消息(按集群)")
    public String queryMessageById(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                   @ToolParam(description = "access key or ak") String ak,
                                   @ToolParam(description = "secret key or sk") String sk,
                                   @ToolParam(description = "集群名称") String clusterName,
                                   @ToolParam(description = "主题") String topic,
                                   @ToolParam(description = "消息ID") String msgId) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryMessage(clusterName, topic, msgId));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "恢复检查半消息")
    public String resumeCheckHalfMessage(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "主题") String topic,
                                         @ToolParam(description = "消息ID") String msgId) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return String.valueOf(admin.resumeCheckHalfMessage(topic, msgId));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消息轨迹详情")
    public String messageTrackDetail(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                     @ToolParam(description = "access key or ak") String ak,
                                     @ToolParam(description = "secret key or sk") String sk,
                                     @ToolParam(description = "消息") String messageJson) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                MessageExt msg = JSON.parseObject(messageJson, MessageExt.class);
                return JSON.toJSONString(admin.messageTrackDetail(msg));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取消息轨迹详情(并发)")
    public String messageTrackDetailConcurrent(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                               @ToolParam(description = "access key or ak") String ak,
                                               @ToolParam(description = "secret key or sk") String sk,
                                               @ToolParam(description = "消息") String messageJson) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                MessageExt msg = JSON.parseObject(messageJson, MessageExt.class);
                return JSON.toJSONString(admin.messageTrackDetailConcurrent(msg));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}
