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
