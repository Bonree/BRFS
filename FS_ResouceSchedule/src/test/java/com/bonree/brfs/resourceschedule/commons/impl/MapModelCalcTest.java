package com.bonree.brfs.resourceschedule.commons.impl;

import static org.junit.Assert.*;

import org.junit.Test;

import com.bonree.brfs.resourceschedule.commons.CommonMapCalcInterface;
import com.bonree.brfs.resourceschedule.model.NetStatModel;

import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MapModelCalcTest {
	
	@Test
	public void testModelCalc(){
		String[] ipList = new String[]{
//			"192.168.4.100",
//			"192.168.4.101",
//			"192.168.4.102",
			"192.168.4.103"
		};
		List<Map<String, NetStatModel>> netList = new ArrayList<Map<String,NetStatModel>>();
		for(int i = 0; i<= 5; i++){
			netList.add(createNetMap(ipList, i *1000));
		}
		CommonMapCalcInterface<String, NetStatModel> nInt = new MapModelCalc<String, NetStatModel>();
		
		Map<String, NetStatModel> netMap = null;
		Map<String, NetStatModel> resultMap = null;
		Map<String,List<NetStatModel>> cMap = null;
		for(Map<String,NetStatModel> map : netList){
			if(netMap == null){
				netMap = map;
				continue;
			}
			resultMap = nInt.calcMapData(map, netMap);
			netMap = map;
			cMap = nInt.collectModels(cMap, resultMap);
		}
		Map<String,NetStatModel> tMap = nInt.sumMapData(cMap);
		System.out.println(tMap);
	}
	
	public static Map<String, NetStatModel> createNetMap(String[] ipList,int initValue){
		Map<String, NetStatModel> netMap = new HashMap<String,NetStatModel>();
		NetStatModel obj = null;
		for(String ip : ipList){
			obj = new NetStatModel();
			obj.setIpAddress(ip);
			obj.setrDataSize(initValue);
			obj.settDataSize(initValue);
			netMap.put(ip, obj);
		}
		return netMap;
	}

}
