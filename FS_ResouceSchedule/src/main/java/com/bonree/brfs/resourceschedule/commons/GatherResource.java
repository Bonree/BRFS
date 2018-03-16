package com.bonree.brfs.resourceschedule.commons;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.hyperic.sigar.SigarException;

import com.bonree.brfs.resourceschedule.config.ResConfig;
import com.bonree.brfs.resourceschedule.model.BaseNetModel;
import com.bonree.brfs.resourceschedule.model.BasePatitionModel;
import com.bonree.brfs.resourceschedule.model.BaseServerModel;
import com.bonree.brfs.resourceschedule.model.NetStatModel;
import com.bonree.brfs.resourceschedule.model.PatitionStatModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.ServerModel;
import com.bonree.brfs.resourceschedule.model.ServerStatModel;
import com.bonree.brfs.resourceschedule.utils.OSCheckUtils;
import com.bonree.brfs.resourceschedule.utils.SigarUtils;
import com.bonree.brfs.resourceschedule.utils.StringUtils;

public class GatherResource {
	
    /**
     * 概述：采集服务基本信息
     * @param serverId
     * @param dataPath
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static BaseServerModel gatherBaseServerInfo(int serverId, String dataPath){
		BaseServerModel obj = new BaseServerModel();
		Map<String, BaseNetModel> netMap = null;
		Map<String, BasePatitionModel> patitionMap = null;
		try {
			netMap = SigarUtils.instance.gatherBaseNetInfos(ResConfig.NET_MAX_R_SPEED, ResConfig.NET_MAX_T_SPEED);
			patitionMap = SigarUtils.instance.gatherBasePatitionInfos(dataPath, ResConfig.DISK_MAX_WRITE_SPEED, ResConfig.DISK_MAX_READ_SPEED);
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
	public static ServerStatModel gatherServerStatInfo(String dataPath){
		ServerStatModel obj = new ServerStatModel();
		Map<String,NetStatModel> netMap = null;
		Map<String, PatitionStatModel> patitionMap = null;
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
	public static ResourceModel calcResourceModel(ServerModel server, List<BaseServerModel> clusterList){
		ResourceModel resource = new ResourceModel();
		return null;
		
	}
}
