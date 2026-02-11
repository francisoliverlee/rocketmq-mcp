package org.apache.rocketmq.mcp.common;

import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class AdminUtil {
    private static final String DEFAULT_NAME_SERVER = System.getProperty("NS_ADDR", System.getenv("NS_ADDR"));
    private static final String DEFAULT_AK = System.getProperty("AK", System.getenv("AK"));
    private static final String DEFAULT_SK = System.getProperty("SK", System.getenv("SK"));

    public static String callAdmin(Function<DefaultMQAdminExt, String> func, String ak, String sk, List<String> nameserverAddressList) throws MQClientException {
        String _ns = (nameserverAddressList == null || nameserverAddressList.isEmpty()) ? DEFAULT_NAME_SERVER : StringUtils.join(nameserverAddressList, ";");
        String _ak = (ak == null || ak.trim().isEmpty()) ? DEFAULT_AK : ak.trim();
        String _sk = (sk == null || sk.trim().isEmpty()) ? DEFAULT_SK : sk.trim();
        DefaultMQAdminExt admin = getAdmin(_ns, _ak, _sk);
        try {
            return func.apply(admin);
        } catch (Exception ex) {
            return ex.getMessage();
        } finally {
            admin.shutdown();
        }
    }

    /**
     * 新的callAdmin方法，返回统一的ApiResponse格式
     */
    public static <T> ApiResponse<T> callAdminWithResponse(Function<DefaultMQAdminExt, T> func, String ak, String sk, List<String> nameserverAddressList) {
        // 参数验证 - 验证必填参数不能为空
        ApiResponse<T> validationResult = validateRequiredParameters(ak, sk, nameserverAddressList);
        if (validationResult != null) {
            return validationResult;
        }

        String _ns = (nameserverAddressList == null || nameserverAddressList.isEmpty()) ? DEFAULT_NAME_SERVER : StringUtils.join(nameserverAddressList, ";");
        String _ak = (ak == null || ak.trim().isEmpty()) ? DEFAULT_AK : ak.trim();
        String _sk = (sk == null || sk.trim().isEmpty()) ? DEFAULT_SK : sk.trim();
        DefaultMQAdminExt admin = null;
        try {
            admin = getAdmin(_ns, _ak, _sk);
            T result = func.apply(admin);
            return ApiResponse.success(result);
        } catch (Exception ex) {
            return ApiResponse.error(ex.getMessage());
        } finally {
            if (admin != null) {
                admin.shutdown();
            }
        }
    }

    /**
     * 验证必填参数不能为空
     */
    private static <T> ApiResponse<T> validateRequiredParameters(String ak, String sk, List<String> nameserverAddressList) {
        if (nameserverAddressList == null || nameserverAddressList.isEmpty()) {
            return ApiResponse.error("nameserverAddressList不能为空");
        }

        if (StringUtils.isBlank(ak)) {
            return ApiResponse.error("ak不能为空");
        }

        if (StringUtils.isBlank(sk)) {
            return ApiResponse.error("sk不能为空");
        }

        return null;
    }

    public static DefaultMQAdminExt getAdmin(String nameserverAddressList, String ak, String sk) throws MQClientException {
        DefaultMQAdminExt admin = null;
        if (StringUtils.isNotBlank(ak) && StringUtils.isNotBlank(sk)) {
            admin = new DefaultMQAdminExt(new AclClientRPCHook(new SessionCredentials(ak, sk)));
        } else {
            admin = new DefaultMQAdminExt();
        }
        admin.setNamesrvAddr(nameserverAddressList);
        admin.start();
        return admin;
    }

}
