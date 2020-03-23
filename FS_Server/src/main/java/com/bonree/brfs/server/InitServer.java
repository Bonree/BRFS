package com.bonree.brfs.server;

import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;

public class InitServer {

    public static void main(String[] args) {
        try {
            String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
            CuratorClient client = CuratorClient.getClientInstance(zkAddresses);
            ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_CLUSTER_NAME), client.getInnerClient());
            CuratorCacheFactory.init(zkAddresses);
            String passwd = null;
            Scanner sc = new Scanner(System.in);
            Thread.sleep(500);
            while (true) {
                System.out.println("please input root user's password:");
                passwd = sc.nextLine();
                if (StringUtils.isEmpty(passwd)) {
                    System.out.println("password is empty!!");
                }else if(passwd.length()<5) {
                    System.out.println("password less 5 size!!");
                }else {
                    System.out.println("password setup successfully!!");
                    break;
                }
            }
            
            SimpleAuthentication authentication = SimpleAuthentication.getAuthInstance(zookeeperPaths.getBaseUserPath(),zookeeperPaths.getBaseLocksPath(), client.getInnerClient());
            UserModel user = new UserModel("root", passwd, (byte)0);
            authentication.createUser(user);
            System.out.println("init server successfully!!");
            sc.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void help() {
        System.out.println("parameter is error!");
    }

}
