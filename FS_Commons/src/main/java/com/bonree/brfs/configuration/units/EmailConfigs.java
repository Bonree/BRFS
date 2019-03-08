package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

import java.util.Calendar;

public class EmailConfigs{
    /**
     * 邮箱服务地址
     */
    public static final ConfigUnit<String> CONFIG_SMTP =
            ConfigUnit.ofString("email.smtp", "mail.bonree.com");
    /**
     * 邮箱服务发送端口
     */
    public static final ConfigUnit<Integer> CONFIG_SMTP_PORT =
            ConfigUnit.ofInt("email.smtp.port", 25);
    /**
     * 邮件发送作者
     */
    public static final ConfigUnit<String> CONFIG_USER =
            ConfigUnit.ofString("email.send.user", "redalert@bonree.com");

    /**
     * 邮件发送作者密码
     */
    public static final ConfigUnit<String> CONFIG_USER_PASSWORD =
            ConfigUnit.ofString("email.send.user.password", "alert!^*90");
    /**
     * 是否ssl
     */
    public static final ConfigUnit<Boolean> CONFIG_USE_SSL =
            ConfigUnit.ofBoolean("email.use_ssl", false);

    /**
     * 收件人地址
     */
    public static final ConfigUnit<String> CONFIG_EMAILS =
            ConfigUnit.ofString("email.recipient", "zhucg@bonree.com,weizheng@bonree.com,chenyp@bonree.com");
    /**
     * 邮件内容表格头
     */
    public static final ConfigUnit<String> CONFIG_HEADER =
            ConfigUnit.ofString("email.header", "BRFS 文件集群");
    /**
     * 程序模块 默认值
     */
    public static final ConfigUnit<String> CONFIG_MODEL =
            ConfigUnit.ofString("email.model", "BRFS 服务");
    /**
     * 公司信息
     */
    public static final ConfigUnit<String> CONFIG_COMPANY =
            ConfigUnit.ofString("email.company", "北京博睿宏远数据科技股份有限公司 版权所有   京 ICP备 08104257 号 京公网安备 1101051190");
    /**
     * 版权信息
     */
    public static final ConfigUnit<String> CONFIG_COPY_RIGHT =
            ConfigUnit.ofString("email.copyright", "Copyright ©2007-${currYear} All rights reserved.".replace("${currYear}", String.valueOf(Calendar.getInstance().get(Calendar.YEAR))));

    /**
     * 并发发送邮件的线程池数
     */
    public static final ConfigUnit<Integer> CONFIG_POOL_SIZE =
            ConfigUnit.ofInt("email.pool.size", 3);



}
