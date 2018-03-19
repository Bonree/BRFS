package com.bonree.brfs.common.zookeeper.curator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.bonree.brfs.common.zookeeper.curator.locking.CuratorLocksClient;
import com.bonree.brfs.common.zookeeper.curator.locking.Executor;

public class LockTest {
    // private static Logger LOG = LoggerFactory.getLogger(LockTest.class);
    public static class MyExecutor implements Executor<String> {

        @Override
        public String execute(CuratorClient client) {
            if (!client.checkExists("/brfs/wz/count")) {
                client.createPersistent("/brfs/wz/count", true, "0".getBytes());
            }
            byte[] bytes = client.getData("/brfs/wz/count");
            
            int result = Integer.parseInt(new String(bytes)) + 1;
            System.out.println(result);
            client.setData("/brfs/wz/count", String.valueOf(result).getBytes());
            return new String(bytes);
        }


    }

    public static void main(String[] args) throws InterruptedException {

        ExecutorService threads = Executors.newFixedThreadPool(10);

        final String lockPath = "/brfs/wz/locks";

        for (int i = 0; i < 10; i++) {
            threads.execute(new Runnable() {

                @Override
                public void run() {
                    CuratorClient client = CuratorClient.getClientInstance("192.168.101.86:2181");
                    CuratorLocksClient<String> lockClient = new CuratorLocksClient<String>(client, lockPath, new MyExecutor(), "testLock");
                    try {
                        lockClient.execute(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    client.close();
                }
            });
        }

        threads.shutdown();
        threads.awaitTermination(10, TimeUnit.MINUTES);

    }
}
