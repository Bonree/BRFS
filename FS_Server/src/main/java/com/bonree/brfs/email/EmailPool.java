package com.bonree.brfs.email;

import com.bonree.brfs.configuration.ConfigObj;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.EmailConfigs;
import com.bonree.mail.worker.MailWorker;
import com.bonree.mail.worker.ProgramInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * 初始化email配置
 */
public class EmailPool{
    private static final Logger LOG = LoggerFactory.getLogger(EmailPool.class);
    private static String COMMA = ",";
    private static EmailPool instance = null;

    public ProgramInfo getProgramInfo(){
        return programInfo;
    }

    private ProgramInfo programInfo = null;
    private ExecutorService pool = null;
    private EmailPool(){
        initEmailBuilder();
    }

    public static EmailPool getInstance(){
        if(instance == null){
            instance = new EmailPool();
        }
        return instance;
    }

    private void initEmailBuilder(){

        ConfigObj conf = Configs.getConfiguration();
        // 初始化email对象信息
        String stmp = conf.GetConfig(EmailConfigs.CONFIG_SMTP);
        int port = conf.GetConfig(EmailConfigs.CONFIG_SMTP_PORT);
        String sendor = conf.GetConfig(EmailConfigs.CONFIG_USER);
        String password = conf.GetConfig(EmailConfigs.CONFIG_USER_PASSWORD);
        boolean sslFlag = conf.GetConfig(EmailConfigs.CONFIG_USE_SSL);
        String emailStr = conf.GetConfig(EmailConfigs.CONFIG_EMAILS);
        String[] emails = StringUtils.split(emailStr, COMMA);
        String header = conf.GetConfig(EmailConfigs.CONFIG_HEADER);
        String model = conf.GetConfig(EmailConfigs.CONFIG_MODEL);
        String company = conf.GetConfig(EmailConfigs.CONFIG_COMPANY);
        String copyright = conf.GetConfig(EmailConfigs.CONFIG_COPY_RIGHT);
        this.programInfo = ProgramInfo.getInstance();
        programInfo.setSmtp(stmp).setPort(port).setUsername(sendor).setPassword(password).setUseSsl(sslFlag).setEmails(emails).setHeader(header).setCommonModel(model).setCompany(company).setCopyRight(copyright);
        // 初始化线程池信息
        int poolSize = conf.GetConfig(EmailConfigs.CONFIG_POOL_SIZE);
        this.pool = Executors.newFixedThreadPool(poolSize);
        showEmailConifg();
    }

    private void showEmailConifg(){
        LOG.info("{}:{}", EmailConfigs.CONFIG_SMTP.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_SMTP));
        LOG.info("{}:{}", EmailConfigs.CONFIG_SMTP_PORT.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_SMTP_PORT));
        LOG.info("{}:{}", EmailConfigs.CONFIG_USER.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_USER));
        LOG.info("{}:{}", EmailConfigs.CONFIG_USER_PASSWORD.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_USER_PASSWORD));
        LOG.info("{}:{}", EmailConfigs.CONFIG_USE_SSL.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_USE_SSL));
        LOG.info("{}:{}", EmailConfigs.CONFIG_EMAILS.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_EMAILS));
        LOG.info("{}:{}", EmailConfigs.CONFIG_HEADER.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_HEADER));
        LOG.info("{}:{}", EmailConfigs.CONFIG_MODEL.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_MODEL));
        LOG.info("{}:{}", EmailConfigs.CONFIG_COMPANY.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_COMPANY));
        LOG.info("{}:{}", EmailConfigs.CONFIG_COPY_RIGHT.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_COPY_RIGHT));
        LOG.info("{}:{}", EmailConfigs.CONFIG_POOL_SIZE.name(), Configs.getConfiguration().GetConfig(EmailConfigs.CONFIG_POOL_SIZE));
    }

    /**
     * 并发发送邮件，避免因为邮件发送耗时阻塞服务
     *
     * @param builder 邮件消息
     */
    public void sendEmail(final MailWorker.Builder builder){
        Runnable callable = new Runnable(){
            private MailWorker mailWorker = builder.build();

            @Override
            public void run(){
                mailWorker.sendEmail();
            }
        };
        this.pool.submit(callable);
    }
}
