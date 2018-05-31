package com.bonree.brfs.server;

import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.authentication.model.UserModel;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.exception.ConfigParseException;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.configuration.Configuration.ConfigPathException;

public class InitServer {

    public static void main(String[] args) {
        String brfsHome = System.getProperty("path");
        try {
            Configuration conf = Configuration.getInstance();
            conf.parse(brfsHome + "/config/server.properties");
            conf.printConfigDetail();
            ServerConfig serverConfig = ServerConfig.parse(conf, brfsHome);
            CuratorClient client = CuratorClient.getClientInstance(serverConfig.getZkHosts());
            ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(serverConfig.getClusterName(), serverConfig.getZkHosts());
            CuratorCacheFactory.init(serverConfig.getZkHosts());
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
            
            SimpleAuthentication authentication = SimpleAuthentication.getAuthInstance(zookeeperPaths.getBaseUserPath(), client.getInnerClient());
            UserModel user = new UserModel("root", passwd, (byte)0);
            authentication.createUser(user);
            System.out.println("init server successfully!!");
            sc.close();
        } catch (ConfigPathException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (ConfigParseException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void help() {
        System.out.println("parameter is error!");
    }

}
