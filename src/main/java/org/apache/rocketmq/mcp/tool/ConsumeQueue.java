package org.apache.rocketmq.mcp.tool;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.mcp.common.ApiResponse;
import org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class ConsumeQueue {
    public static final List<String> WRITE_OPERATIONS = List.of(
            "cleanExpiredConsumerQueue",
            "cleanExpiredConsumerQueueByAddr"
    );

    @Tool(description = "检查RocksDB CQ写入进度")
    public ApiResponse<Object> checkRocksdbCqWriteProgress(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                           @ToolParam(description = "access key or ak") String ak,
                                                           @ToolParam(description = "secret key or sk") String sk,
                                                           @ToolParam(description = "broker地址") String brokerAddr,
                                                           @ToolParam(description = "主题名称") String topic,
                                                           @ToolParam(description = "检查存储时间") long checkStoreTime) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.checkRocksdbCqWriteProgress(brokerAddr, topic, checkStoreTime);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消费队列数据")
    public ApiResponse<Object> queryConsumeQueue(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "broker地址") String brokerAddr,
                                                 @ToolParam(description = "主题") String topic,
                                                 @ToolParam(description = "队列ID") int queueId,
                                                 @ToolParam(description = "起始索引") long index,
                                                 @ToolParam(description = "数量") int count,
                                                 @ToolParam(description = "消费者组") String consumerGroup) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.queryConsumeQueue(brokerAddr, topic, queueId, index, count, consumerGroup);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "导出RocksDB配置到JSON")
    public ApiResponse<String> exportRocksDBConfigToJson(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                         @ToolParam(description = "access key or ak") String ak,
                                                         @ToolParam(description = "secret key or sk") String sk,
                                                         @ToolParam(description = "broker地址") String brokerAddr,
                                                         @ToolParam(description = "配置类型列表") List<String> configTypes) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> types = configTypes.stream()
                        .map(ExportRocksDBConfigToJsonRequestHeader.ConfigType::valueOf)
                        .collect(Collectors.toList());
                admin.exportRocksDBConfigToJson(brokerAddr, types);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理过期消费队列")
    public ApiResponse<String> cleanExpiredConsumerQueue(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                         @ToolParam(description = "access key or ak") String ak,
                                                         @ToolParam(description = "secret key or sk") String sk,
                                                         @ToolParam(description = "集群名称") String cluster) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.cleanExpiredConsumerQueue(cluster);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理过期消费队列(按地址)")
    public ApiResponse<String> cleanExpiredConsumerQueueByAddr(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                               @ToolParam(description = "access key or ak") String ak,
                                                               @ToolParam(description = "secret key or sk") String sk,
                                                               @ToolParam(description = "broker地址") String addr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.cleanExpiredConsumerQueueByAddr(addr);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

}