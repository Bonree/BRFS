package com.bonree.brfs.common.zookeeper.curator;

public class Test {

    public static void main(String[] args) throws InterruptedException {
        //        ExecutorService threads = Executors.newFixedThreadPool(5);
        //        
        //        for(int i = 0;i<10;i++) {
        //            final int index = i;
        //            threads.execute(new Runnable() {
        //                
        //                @Override
        //                public void run() {
        //                    CuratorClient client = CuratorClient.getClientInstance("192.168.101.86:2181");
        //                    System.out.println("/brfs/wz/cache/"+index);
        ////                    client.createEphemeral("/brfs/wz/cache/"+index, false);
        //                    client.createPersistent("/brfs/wz/cache/"+index, false);
        ////                    client.delete("/brfs/wz/cache/"+index, false);
        //                    client.close();
        //                }
        //            });
        //        }
        //        
        //        threads.shutdown();
        //        threads.awaitTermination(1, TimeUnit.DAYS);
        CuratorClient client = CuratorClient.getClientInstance("192.168.101.86:2181");
        client.setData("/brfs/wz/cache/6", "1213213".getBytes());
        client.close();
        //        client.createPersistent("/brfs/wz/cache/aaaa", true);
        //        client.delete("/brfs/wz/cache/aaaa",false);
        //        client.createEphemeral("/brfs/wz/cache/aaaa", true);

    }

}
