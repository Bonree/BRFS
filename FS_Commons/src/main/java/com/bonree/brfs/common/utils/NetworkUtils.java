package com.bonree.brfs.common.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

import com.google.common.collect.ImmutableList;

public final class NetworkUtils {

    /**
     * 获取所有的本机IP地址
     * 
     * @return 字符串形式的ip地址列表
     */
    public static List<InetAddress> getAllLocalIps() {
        ImmutableList.Builder<InetAddress> addresses = ImmutableList.builder();
        try {
            Enumeration<NetworkInterface> allInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = allInterfaces.nextElement();
                if(networkInterface.isLoopback()
                        || networkInterface.isVirtual()
                        || !networkInterface.isUp()) {
                    continue;
                }

                Enumeration<InetAddress> adddresses = networkInterface.getInetAddresses();
                while (adddresses.hasMoreElements()) {
                    InetAddress address = adddresses.nextElement();
                    if ((address != null)
                            && !address.isLoopbackAddress()
                            && (networkInterface.isPointToPoint() || !address.isLinkLocalAddress())) {
                        addresses.add(address);
                    }
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        return addresses.build();
    }

    private NetworkUtils() {
    }
}
