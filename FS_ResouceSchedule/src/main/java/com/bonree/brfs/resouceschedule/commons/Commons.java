package com.bonree.brfs.resouceschedule.commons;

import java.io.File;
import java.util.Map;

import org.hyperic.sigar.SigarException;

import com.bonree.brfs.resouceschedule.utils.Globals;
import com.bonree.brfs.resouceschedule.utils.OSCheck;
import com.bonree.brfs.resouceschedule.utils.StringUtils;
import com.bonree.brfs.resouceschedule.vo.BaseNetInfo;
import com.bonree.brfs.resouceschedule.vo.BasePatitionInfo;
import com.bonree.brfs.resouceschedule.vo.BaseServerInfo;
import com.bonree.brfs.resouceschedule.vo.NetStatInfo;
import com.bonree.brfs.resouceschedule.vo.PatitionStatInfo;
import com.bonree.brfs.resouceschedule.vo.ServerStatInfo;

public class Commons {
	 /**
     * 概述：过滤非法的挂载点
     * @param mountPoint 文件分区挂载点
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static Boolean filterMountPoint(String mountPoint){
    	// 1.挂载点为空返回NULL
    	if(StringUtils.isEmpty(mountPoint)){
    		return true;
    	}
    	File mountFile = new File(mountPoint);
    	// 2.目录不存在返回NULL
    	if(!mountFile.exists()){
    		return true;
    	}
    	// 3.挂载点为文件返回NULL
    	if(mountFile.isFile()){
    		return true;
    	}
    	return false;
    }
    /**
     * 概述：过滤非法的ip地址
     * @param ip
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static Boolean filterIp(String ip){
    	// 1.过滤为空的ip
        if(StringUtils.isEmpty(ip)){
            return true;
        }
        // 2.过滤长度超过的ip地址
        if(ip.length() > 15){
        	return true;
        }
        String[] ipEles = StringUtils.getSplit(ip, ".");
        // 3.过滤格式不对的ip地址
        if(ipEles.length != 4){
        	return true;
        }
        // 4.过滤内容不对的ip地址
        for(String ipEle : ipEles){
        	if(!StringUtils.isMathNumeric(ipEle)){
        		return true;
        	}
        	long value = Long.valueOf(ipEle);
        	if(value < 0 || value >255){
        		return true;
        	}
        }
        // 5.过滤回环地址
        if("127.0.0.1".equals(ip)){
            return true;
        }
        // 6.过滤空地址
        if("0.0.0.0".equals(ip)){
            return true;
        }
        return false;
    }
	 /**
     * 概述：sigar配置path，在使用sigar前必须引用第三方依赖包，否则程序报错
     * @param installDir
     * @throws Exception
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static void  loadLibraryPath(String libPath) throws Exception{
        if(StringUtils.isEmpty(libPath)){
        	throw new NullPointerException("[config error] sigar lib path is empty !!!");
        }
        File file = new File(libPath);
        if(!file.exists()){
        	throw new NullPointerException("[config error] sigar lib path is not exists !!! path : "+ libPath);
        }
    	String path = System.getProperty("java.library.path");
        if(OSCheck.getOperatingSystemType() == OSCheck.OSType.Windows){
            path += ";" + libPath;
        }else{
            path += ":" + libPath;
        }
        System.setProperty("java.library.path",path);
    }
    /**
     * 概述：采集服务基本信息
     * @param serverId
     * @param dataPath
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static BaseServerInfo gatherBaseServerInfo(int serverId, String dataPath){
		BaseServerInfo obj = new BaseServerInfo();
		Map<String, BaseNetInfo> netMap = null;
		Map<String, BasePatitionInfo> patitionMap = null;
		try {
			netMap = SigarUtils.instance.gatherBaseNetInfos(Globals.NET_MAX_R_SPEED, Globals.NET_MAX_T_SPEED);
			patitionMap = SigarUtils.instance.gatherBasePatitionInfos(dataPath, Globals.DISK_MAX_WRITE_SPEED, Globals.DISK_MAX_READ_SPEED);
			obj.setCpuCoreCount(SigarUtils.instance.gatherCpuCoreCount());
			obj.setMemorySize(SigarUtils.instance.gatherMemSize());
			obj.setServerId(serverId);
			if(netMap != null){
				obj.setNetInfoMap(netMap);
			}
			if(patitionMap != null){
				obj.setPatitionInfoMap(patitionMap);
			}
		}
		catch (SigarException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return obj;
	}
    /**
     * 概述：采集状态信息
     * @param dataPath
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
	public static ServerStatInfo gatherServerStatInfo(String dataPath){
		ServerStatInfo obj = new ServerStatInfo();
		Map<String,NetStatInfo> netMap = null;
		Map<String, PatitionStatInfo> patitionMap = null;
		try {
			netMap = SigarUtils.instance.gatherNetStatInfos();
			patitionMap = SigarUtils.instance.gatherPatitionStatInfos("E:/");
			obj.setCpuStatInfo(SigarUtils.instance.gatherCpuStatInfo());
			obj.setMemoryStatInfo(SigarUtils.instance.gatherMemoryStatInfo());
			if(netMap != null){
				obj.setNetStatInfoMap(netMap);
			}
			if(patitionMap != null){
				obj.setPatitionStatInfoMap(patitionMap);
			}
		}
		catch (SigarException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return obj;
	}
}
