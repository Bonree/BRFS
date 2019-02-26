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
    private ExecutorService pool = null;
    private EmailPool(){
        initEmailBuilder();
    }
    public static EmailPool getInstance(){
        return new EmailPool();
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
        String[] emails = StringUtils.split(emailStr,COMMA);
        String header = conf.GetConfig(EmailConfigs.CONFIG_HEADER);
        String model = conf.GetConfig(EmailConfigs.CONFIG_MODEL);
        String company = conf.GetConfig(EmailConfigs.CONFIG_COMPANY);
        String copyright = conf.GetConfig(EmailConfigs.CONFIG_COPY_RIGHT);
        ProgramInfo programInfo = ProgramInfo.getInstance();
        programInfo.setSmtp(stmp)
                .setPort(port)
                .setUsername(sendor)
                .setPassword(password)
                .setUseSsl(sslFlag)
                .setEmails(emails)
                .setHeader(header)
                .setCommonModel(model)
                .setCompany(company)
                .setCopyRight(copyright);
        // 初始化线程池信息
        int poolSize = conf.GetConfig(EmailConfigs.CONFIG_POOL_SIZE);
        this.pool = Executors.newFixedThreadPool(poolSize);

    }

    /**
     * 并发发送邮件，避免因为邮件发送耗时阻塞服务
     * @param builder 邮件消息
     * @param waitResult 等待发送结果
     */
    public boolean sendEmail(final MailWorker.Builder builder,boolean waitResult){
        Callable<Boolean> callable = new Callable<Boolean>(){
            @Override
            public Boolean call() throws Exception{
                return builder.build().sendEmail();
            }
        };
        Future<Boolean> future = this.pool.submit(callable);
        boolean rFlag = true;
        if(waitResult){
            try{
                rFlag = future.get();
            } catch(InterruptedException | ExecutionException e){
                LOG.error("send mail happen exception {}",e);
                rFlag = false;
            }
        }
        return rFlag;
    }
}
