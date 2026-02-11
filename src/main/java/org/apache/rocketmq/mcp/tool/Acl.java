package org.apache.rocketmq.mcp.tool;

import java.util.List;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.mcp.common.AdminUtil;
import org.apache.rocketmq.mcp.common.ApiResponse;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;

@org.springframework.stereotype.Service
public class Acl {
    public static final List<String> WRITE_OPERATIONS = List.of(
            "createAndUpdatePlainAccessConfig",
            "deletePlainAccessConfig",
            "updateGlobalWhiteAddrConfig",
            "updateGlobalWhiteAddrConfigWithAcl",
            "createUser",
            "updateUser",
            "deleteUser",
            "createAcl",
            "updateAcl",
            "deleteAcl"
    );

    @Tool(description = "创建和更新acl配置")
    public ApiResponse<String> createAndUpdatePlainAccessConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                @ToolParam(description = "access key or ak") String ak,
                                                                @ToolParam(description = "secret key or sk") String sk,
                                                                @ToolParam(description = "普通访问配置") PlainAccessConfig plainAccessConfig) {
        return AdminUtil.callAdminWithResponse(admin -> {
            ClusterInfo clusterInfo = null;
            try {
                clusterInfo = admin.examineBrokerClusterInfo();
            } catch (Exception ex) {
                return "[fail] call examineBrokerClusterInfo exception: " + ExceptionUtils.getStackTrace(ex);
            }

            if (clusterInfo == null || clusterInfo.getBrokerAddrTable() == null || clusterInfo.getBrokerAddrTable().isEmpty()) {
                return "[fail] clusterInfo or brokerAddrTable is null";
            }
            try {
                admin.createAndUpdatePlainAccessConfig(clusterInfo.getBrokerAddrTable().keySet().iterator().next(), plainAccessConfig);
                return "[success] acl config created/updated";
            } catch (Exception e) {
                return "[fail] createAndUpdatePlainAccessConfig exception: " + ExceptionUtils.getStackTrace(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除acl配置")
    public ApiResponse<String> deletePlainAccessConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                       @ToolParam(description = "access key or ak") String ak,
                                                       @ToolParam(description = "secret key or sk") String sk,
                                                       @ToolParam(description = "broker地址") String addr,
                                                       @ToolParam(description = "访问密钥") String accessKey) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.deletePlainAccessConfig(addr, accessKey);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新acl全局白名单ip地址")
    public ApiResponse<String> updateGlobalWhiteAddrConfig(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                           @ToolParam(description = "access key or ak") String ak,
                                                           @ToolParam(description = "secret key or sk") String sk,
                                                           @ToolParam(description = "broker地址") String addr,
                                                           @ToolParam(description = "全局白名单地址") String globalWhiteAddrs) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.updateGlobalWhiteAddrConfig(addr, globalWhiteAddrs);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新acl全局白名单地址ip")
    public ApiResponse<String> updateGlobalWhiteAddrConfigWithAcl(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                                  @ToolParam(description = "access key or ak") String ak,
                                                                  @ToolParam(description = "secret key or sk") String sk,
                                                                  @ToolParam(description = "broker地址") String addr,
                                                                  @ToolParam(description = "全局白名单地址") String globalWhiteAddrs,
                                                                  @ToolParam(description = "ACL文件完整路径") String aclFileFullPath) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.updateGlobalWhiteAddrConfig(addr, globalWhiteAddrs, aclFileFullPath);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取broker中的全部acl版本配置信息")
    public ApiResponse<Object> getAclVersionList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                                 @ToolParam(description = "access key or ak") String ak,
                                                 @ToolParam(description = "secret key or sk") String sk,
                                                 @ToolParam(description = "broker地址") String addr) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.examineBrokerClusterAclVersionInfo(addr);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建用户")
    public ApiResponse<String> createUser(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "broker地址") String brokerAddr,
                                          @ToolParam(description = "用户名") String username,
                                          @ToolParam(description = "密码") String password,
                                          @ToolParam(description = "用户类型") String userType) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.createUser(brokerAddr, username, password, userType);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新用户")
    public ApiResponse<String> updateUser(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "broker地址") String brokerAddr,
                                          @ToolParam(description = "用户名") String username,
                                          @ToolParam(description = "密码") String password,
                                          @ToolParam(description = "用户类型") String userType,
                                          @ToolParam(description = "用户状态") String userStatus) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.updateUser(brokerAddr, username, password, userType, userStatus);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除用户")
    public ApiResponse<String> deleteUser(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "broker地址") String brokerAddr,
                                          @ToolParam(description = "用户名") String username) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.deleteUser(brokerAddr, username);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取用户信息")
    public ApiResponse<Object> getUser(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                       @ToolParam(description = "access key or ak") String ak,
                                       @ToolParam(description = "secret key or sk") String sk,
                                       @ToolParam(description = "broker地址") String brokerAddr,
                                       @ToolParam(description = "用户名") String username) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getUser(brokerAddr, username);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "查询全部用户信息")
    public ApiResponse<Object> getAllUsers(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                           @ToolParam(description = "access key or ak") String ak,
                                           @ToolParam(description = "secret key or sk") String sk,
                                           @ToolParam(description = "broker地址") String brokerAddr,
                                           @ToolParam(description = "过滤条件") String filter) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.listUser(brokerAddr, filter);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "创建ACL")
    public ApiResponse<String> createAcl(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "broker地址") String brokerAddr,
                                         @ToolParam(description = "主体") String subject,
                                         @ToolParam(description = "资源列表") List<String> resources,
                                         @ToolParam(description = "操作列表") List<String> actions,
                                         @ToolParam(description = "源IP列表") List<String> sourceIps,
                                         @ToolParam(description = "决策") String decision) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.createAcl(brokerAddr, subject, resources, actions, sourceIps, decision);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "更新ACL")
    public ApiResponse<String> updateAcl(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "broker地址") String brokerAddr,
                                         @ToolParam(description = "主体") String subject,
                                         @ToolParam(description = "资源列表") List<String> resources,
                                         @ToolParam(description = "操作列表") List<String> actions,
                                         @ToolParam(description = "源IP列表") List<String> sourceIps,
                                         @ToolParam(description = "决策") String decision) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.updateAcl(brokerAddr, subject, resources, actions, sourceIps, decision);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "删除ACL")
    public ApiResponse<String> deleteAcl(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                         @ToolParam(description = "access key or ak") String ak,
                                         @ToolParam(description = "secret key or sk") String sk,
                                         @ToolParam(description = "broker地址") String brokerAddr,
                                         @ToolParam(description = "主体") String subject,
                                         @ToolParam(description = "资源") String resource) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                admin.deleteAcl(brokerAddr, subject, resource);
                return "success";
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取ACL信息")
    public ApiResponse<Object> getAcl(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                      @ToolParam(description = "access key or ak") String ak,
                                      @ToolParam(description = "secret key or sk") String sk,
                                      @ToolParam(description = "broker地址") String brokerAddr,
                                      @ToolParam(description = "主体") String subject) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.getAcl(brokerAddr, subject);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

    @Tool(description = "获取全部的acl配置信息")
    public ApiResponse<Object> getAclList(@ToolParam(description = "nameserver/namesrv 地址列表") List<String> nameserverAddressList,
                                          @ToolParam(description = "access key or ak") String ak,
                                          @ToolParam(description = "secret key or sk") String sk,
                                          @ToolParam(description = "broker地址") String brokerAddr,
                                          @ToolParam(description = "主体过滤条件") String subjectFilter,
                                          @ToolParam(description = "资源过滤条件") String resourceFilter) {
        return AdminUtil.callAdminWithResponse(admin -> {
            try {
                return admin.listAcl(brokerAddr, subjectFilter, resourceFilter);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ak, sk, nameserverAddressList);
    }

}