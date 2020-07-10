package com.bonree.email;

import com.bonree.brfs.email.EmailPool;
import com.bonree.mail.worker.MailWorker;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmailPoolTest {
    private static final Logger LOG = LoggerFactory.getLogger(EmailPoolTest.class);

    /**
     * 初始化配置文件信息
     */
    @Before
    public void initConfig() {
        String resourcePath = Class.class.getResource("/").getPath() + "/server.properties";
        System.setProperty("configuration.file", resourcePath);
    }

    @Test
    @SuppressWarnings("all")
    public void initPool() {
        EmailPool.getInstance();
    }

    @Test
    @SuppressWarnings("all")
    public void sendmail() {
        MailWorker.Builder builder =
            MailWorker.newBuilder(EmailPool.getInstance().getProgramInfo()).setException(new NullPointerException("none"));
        EmailPool.getInstance().sendEmail(builder);
    }

    @Test
    @SuppressWarnings("all")
    public void sendmailWaitResult() {
        MailWorker.Builder builder =
            MailWorker.newBuilder(EmailPool.getInstance().getProgramInfo()).setException(new NullPointerException("none"));
        EmailPool.getInstance().sendEmail(builder);
    }

    @Test
    @SuppressWarnings("all")
    public void sendmailWaitResultNOException() {
        MailWorker.Builder builder = MailWorker.newBuilder(EmailPool.getInstance().getProgramInfo()).setMessage("我是谁");
        EmailPool.getInstance().sendEmail(builder);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
