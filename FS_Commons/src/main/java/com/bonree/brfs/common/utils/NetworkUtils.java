package com.bonree.brfs.common.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

import com.google.common.collect.Lists;

public final class NetworkUtils {
	
	/**
	 * 获取所有的本机IP地址
	 * 
	 * @return 字符串形式的ip地址列表
	 */
	public static List<InetAddress> getAllLocalIps() {
        List<InetAddress> listAdr = Lists.newArrayList();
        try {
        	Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
            if (nifs == null) return listAdr;

            while (nifs.hasMoreElements())
            {
                NetworkInterface nif = nifs.nextElement();
                // We ignore subinterfaces - as not yet needed.

                Enumeration<InetAddress> adrs = nif.getInetAddresses();
                while ( adrs.hasMoreElements() )
                {
                    InetAddress adr = adrs.nextElement();
                    if ((adr != null) && !adr.isLoopbackAddress() && (nif.isPointToPoint() || !adr.isLinkLocalAddress()))
                    {
                        listAdr.add(adr);
                    }
                }
            }
		} catch (SocketException e) {
			e.printStackTrace();
		}
        
        return listAdr;
    }
	
	private NetworkUtils() {}
}
