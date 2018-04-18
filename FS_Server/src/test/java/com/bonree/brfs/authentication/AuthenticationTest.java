package com.bonree.brfs.authentication;

import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;

public class AuthenticationTest {

    public static void main(String[] args) throws InterruptedException {
        String zkUrl = "192.168.101.86:2181";
        CuratorCacheFactory.init(zkUrl);
        deleteNode(zkUrl);
//        testOpt();
    }

    public static void deleteNode(String zkUrl) {
        CuratorClient curatorClient = CuratorClient.getClientInstance(zkUrl);
        curatorClient.delete("/wz_test11", true);
        curatorClient.close();
    }

    public static void testEfficiency() {
        String basePath = "/brfs/wz/auth/user";
        String zkUrl = "192.168.101.86:2181";

        SimpleAuthentication authentication = SimpleAuthentication.getAuthInstance(basePath, zkUrl);
        long b = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            authentication.auth("root:root");
        }
        System.out.println("use time:" + (System.currentTimeMillis() - b));
    }

    public static void testOpt() throws InterruptedException {
        String basePath = "/brfs/wz/auth/user";
        String zkUrl = "192.168.101.86:2181";
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        if (!client.checkExists(basePath)) {
            client.createPersistent(basePath, true);
        }
        client.close();

        SimpleAuthentication authentication = SimpleAuthentication.getAuthInstance(basePath, zkUrl);
        UserModel user = new UserModel("weizheng3", "weizheng3", (byte) 66);
        user.setDescription("weizheng3");

        authentication.createUser(user);

        Thread.sleep(1000);
        System.out.println(authentication.auth("weizheng3:weizheng3"));

        user.setPasswd("wz3");

        authentication.updateUser(user);
        Thread.sleep(1000);
        System.out.println(authentication.auth("weizheng3:wz3"));

        authentication.deleteUser("weizheng3");
        Thread.sleep(1000);
        System.out.println(authentication.auth("weizheng3:wz3"));
        Thread.sleep(Long.MAX_VALUE);
    }
}
