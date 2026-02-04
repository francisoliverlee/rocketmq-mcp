package org.apache.rocketmq.mcp.tool;

import com.alibaba.fastjson2.JSON;
import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class Acl {
    @Tool(description = "创建和更新acl配置")
    public String createAndUpdatePlainAccessConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                   @ToolParam(description = "access key or ak") String ak,
                                                   @ToolParam(description = "secret key or sk") String sk,
                                                   @ToolParam(description = "broker地址") String addr,
                                                   @ToolParam(description = "普通访问配置") PlainAccessConfig plainAccessConfig) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.createAndUpdatePlainAccessConfig(addr, plainAccessConfig);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除acl配置")
    public String deletePlainAccessConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "broker地址") String addr,
                                          @ToolParam(description = "访问密钥") String accessKey) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.deletePlainAccessConfig(addr, accessKey);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新acl全局白名单ip地址")
    public String updateGlobalWhiteAddrConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                              @ToolParam(description = "access key or ak") String ak,
                                              @ToolParam(description = "secret key or sk") String sk,
                                              @ToolParam(description = "broker地址") String addr,
                                              @ToolParam(description = "全局白名单地址") String globalWhiteAddrs) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.updateGlobalWhiteAddrConfig(addr, globalWhiteAddrs);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新acl全局白名单地址ip")
    public String updateGlobalWhiteAddrConfigWithAcl(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                     @ToolParam(description = "access key or ak") String ak,
                                                     @ToolParam(description = "secret key or sk") String sk,
                                                     @ToolParam(description = "broker地址") String addr,
                                                     @ToolParam(description = "全局白名单地址") String globalWhiteAddrs,
                                                     @ToolParam(description = "ACL文件完整路径") String aclFileFullPath) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.updateGlobalWhiteAddrConfig(addr, globalWhiteAddrs, aclFileFullPath);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取broker中的全部acl版本配置信息")
    public String getAclVersionList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                    @ToolParam(description = "access key or ak") String ak,
                                    @ToolParam(description = "secret key or sk") String sk,
                                    @ToolParam(description = "broker地址") String addr) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.examineBrokerClusterAclVersionInfo(addr));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建用户")
    public String createUser(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                             @ToolParam(description = "access key or ak") String ak,
                             @ToolParam(description = "secret key or sk") String sk,
                             @ToolParam(description = "broker地址") String brokerAddr,
                             @ToolParam(description = "用户名") String username,
                             @ToolParam(description = "密码") String password,
                             @ToolParam(description = "用户类型") String userType) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.createUser(brokerAddr, username, password, userType);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新用户")
    public String updateUser(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                             @ToolParam(description = "access key or ak") String ak,
                             @ToolParam(description = "secret key or sk") String sk,
                             @ToolParam(description = "broker地址") String brokerAddr,
                             @ToolParam(description = "用户名") String username,
                             @ToolParam(description = "密码") String password,
                             @ToolParam(description = "用户类型") String userType,
                             @ToolParam(description = "用户状态") String userStatus) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.updateUser(brokerAddr, username, password, userType, userStatus);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除用户")
    public String deleteUser(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                             @ToolParam(description = "access key or ak") String ak,
                             @ToolParam(description = "secret key or sk") String sk,
                             @ToolParam(description = "broker地址") String brokerAddr,
                             @ToolParam(description = "用户名") String username) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.deleteUser(brokerAddr, username);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取用户信息")
    public String getUser(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                          @ToolParam(description = "access key or ak") String ak,
                          @ToolParam(description = "secret key or sk") String sk,
                          @ToolParam(description = "broker地址") String brokerAddr,
                          @ToolParam(description = "用户名") String username) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getUser(brokerAddr, username));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询全部用户信息")
    public String getAllUsers(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                              @ToolParam(description = "access key or ak") String ak,
                              @ToolParam(description = "secret key or sk") String sk,
                              @ToolParam(description = "broker地址") String brokerAddr,
                              @ToolParam(description = "过滤条件") String filter) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.listUser(brokerAddr, filter));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建ACL")
    public String createAcl(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                            @ToolParam(description = "access key or ak") String ak,
                            @ToolParam(description = "secret key or sk") String sk,
                            @ToolParam(description = "broker地址") String brokerAddr,
                            @ToolParam(description = "主体") String subject,
                            @ToolParam(description = "资源列表") List<String> resources,
                            @ToolParam(description = "操作列表") List<String> actions,
                            @ToolParam(description = "源IP列表") List<String> sourceIps,
                            @ToolParam(description = "决策") String decision) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.createAcl(brokerAddr, subject, resources, actions, sourceIps, decision);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新ACL")
    public String updateAcl(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                            @ToolParam(description = "access key or ak") String ak,
                            @ToolParam(description = "secret key or sk") String sk,
                            @ToolParam(description = "broker地址") String brokerAddr,
                            @ToolParam(description = "主体") String subject,
                            @ToolParam(description = "资源列表") List<String> resources,
                            @ToolParam(description = "操作列表") List<String> actions,
                            @ToolParam(description = "源IP列表") List<String> sourceIps,
                            @ToolParam(description = "决策") String decision) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.updateAcl(brokerAddr, subject, resources, actions, sourceIps, decision);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }


    @Tool(description = "删除ACL")
    public String deleteAcl(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                            @ToolParam(description = "access key or ak") String ak,
                            @ToolParam(description = "secret key or sk") String sk,
                            @ToolParam(description = "broker地址") String brokerAddr,
                            @ToolParam(description = "主体") String subject,
                            @ToolParam(description = "资源") String resource) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                admin.deleteAcl(brokerAddr, subject, resource);
                return "success";
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取ACL信息")
    public String getAcl(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                         @ToolParam(description = "access key or ak") String ak,
                         @ToolParam(description = "secret key or sk") String sk,
                         @ToolParam(description = "broker地址") String brokerAddr,
                         @ToolParam(description = "主体") String subject) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.getAcl(brokerAddr, subject));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取全部的acl配置信息")
    public String getAclList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                             @ToolParam(description = "access key or ak") String ak,
                             @ToolParam(description = "secret key or sk") String sk,
                             @ToolParam(description = "broker地址") String brokerAddr,
                             @ToolParam(description = "主体过滤条件") String subjectFilter,
                             @ToolParam(description = "资源过滤条件") String resourceFilter) throws MQClientException {
        return AdminUtil.callAdmin(admin -> {
            try {
                return JSON.toJSONString(admin.listAcl(brokerAddr, subjectFilter, resourceFilter));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, ak, sk, nameserverAddressList);
    }

}
