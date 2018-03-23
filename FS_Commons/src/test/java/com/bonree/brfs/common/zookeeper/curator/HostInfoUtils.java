package com.bonree.brfs.common.zookeeper.curator;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostInfoUtils {

    public static InetAddress getInetAddress() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            System.out.println("unknown host!");
        }
        return null;
    }

    public static String getHostIp(InetAddress netAddress) {
        if (null == netAddress) {
            return null;
        }
        String ip = netAddress.getHostAddress(); // get the ip address
        return ip;
    }

    public static String getHostName(InetAddress netAddress) {
        if (null == netAddress) {
            return null;
        }
        String name = netAddress.getHostName();
        return name;
    }

    public static void main(String[] atgs) {
        InetAddress address = getInetAddress();
        System.out.println(getHostIp(address));
        System.out.println(getHostName(address));
    }

}
