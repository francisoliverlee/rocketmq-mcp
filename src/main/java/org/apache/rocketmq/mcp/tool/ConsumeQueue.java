package org.apache.rocketmq.mcp.tool;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.remoting.protocol.header.ExportRocksDBConfigToJsonRequestHeader;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

import java.util.List;
import java.util.stream.Collectors;

@org.springframework.stereotype.Service
public class ConsumeQueue {

    @Tool(description = "检查RocksDB CQ写入进度")
    public String checkRocksdbCqWriteProgress(@ToolParam(description = "nameserver/namesrv 地址列表") String nameserverAddressList,
                                              @ToolParam(description = "access key or ak") String ak,
                                              @ToolParam(description = "secret key or sk") String sk,
                                              @ToolParam(description = "broker地址") String brokerAddr,
                                              @ToolParam(description = "主题名称") String topic,
                                              @ToolParam(description = "检查存储时间") long checkStoreTime) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.checkRocksdbCqWriteProgress(brokerAddr, topic, checkStoreTime));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询消费队列数据")
    public String queryConsumeQueue(@ToolParam(description = "nameserver/namesrv 地址列表") String nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "broker地址") String brokerAddr,
                                    @ToolParam(description = "主题") String topic,
                                    @ToolParam(description = "队列ID") int queueId,
                                    @ToolParam(description = "起始索引") long index,
                                    @ToolParam(description = "数量") int count,
                                    @ToolParam(description = "消费者组") String consumerGroup) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.queryConsumeQueue(brokerAddr, topic, queueId, index, count, consumerGroup));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "导出RocksDB配置到JSON")
    public String exportRocksDBConfigToJson(@ToolParam(description = "nameserver/namesrv 地址列表") String nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "broker地址") String brokerAddr,
                                            @ToolParam(description = "配置类型列表") List<String> configTypes) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                List<ExportRocksDBConfigToJsonRequestHeader.ConfigType> types = configTypes.stream()
                        .map(ExportRocksDBConfigToJsonRequestHeader.ConfigType::valueOf)
                        .collect(Collectors.toList());
                admin.exportRocksDBConfigToJson(brokerAddr, types);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理过期消费队列")
    public String cleanExpiredConsumerQueue(@ToolParam(description = "nameserver/namesrv 地址列表") String nameserverAddressList,
                                            @ToolParam(description = "access key or ak") String ak,
                                            @ToolParam(description = "secret key or sk") String sk,
                                            @ToolParam(description = "集群名称") String cluster) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return String.valueOf(admin.cleanExpiredConsumerQueue(cluster));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "清理过期消费队列(按地址)")
    public String cleanExpiredConsumerQueueByAddr(@ToolParam(description = "nameserver/namesrv 地址列表") String nameserverAddressList,
                                                  @ToolParam(description = "access key or ak") String ak,
                                                  @ToolParam(description = "secret key or sk") String sk,
                                                  @ToolParam(description = "broker地址") String addr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return String.valueOf(admin.cleanExpiredConsumerQueueByAddr(addr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}
