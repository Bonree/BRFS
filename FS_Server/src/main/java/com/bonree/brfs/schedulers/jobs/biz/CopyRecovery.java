
package com.bonree.brfs.schedulers.jobs.biz;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.LocalDiskNodeClient;
import com.bonree.brfs.duplication.datastream.connection.http.HttpDiskNodeConnection;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.rebalance.route.SecondIDParser;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.AtomTaskResultModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.server.identification.ServerIDManager;

public class CopyRecovery {
	private static final Logger LOG = LoggerFactory.getLogger(CopyRecovery.class);
	/**
	 * 概述：修复目录
	 * @param content
	 * @param zkHosts
	 * @param baseRoutesPath
	 * @param taskName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static TaskResultModel recoveryDirs(String content, String zkHosts, String baseRoutesPath,String dataPath) {
		TaskResultModel result = new TaskResultModel();
		BatchAtomModel batch = converStringToBatch(content);
		if(batch == null){
			result.setSuccess(false);
			LOG.debug("<recoveryDirs> batch is empty");
			return result;
		}
		List<AtomTaskModel> atoms = batch.getAtoms();
		if(atoms == null|| atoms.isEmpty()){
			result.setSuccess(true);
			LOG.debug("<recoveryDirs> file is empty");
			return result;
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		ServerIDManager sim = mcf.getSim();
		ServiceManager sm = mcf.getSm();
		Service localServer = sm.getServiceById(mcf.getGroupName(), mcf.getServerId());
		StorageNameManager snm = mcf.getSnm();
		
		DiskNodeClient client = new LocalDiskNodeClient();
		CuratorClient curatorClient = CuratorClient.getClientInstance(zkHosts);
		StorageNameNode sn = null;
		SecondIDParser parser = null;
		String snName = null;
		int snId = 0;
		String snSId = null;
		AtomTaskResultModel atomR = null;
		List<String> errors = null;
		for (AtomTaskModel atom : atoms) {
			atomR = new AtomTaskResultModel();
			atomR.setFiles(atom.getFiles());
			atomR.setDir(atom.getDirName());
			atomR.setSn(atom.getStorageName());
			snName = atom.getStorageName();
			sn = snm.findStorageName(snName);
			if (sn == null) {
				atomR.setSuccess(false);
				result.setSuccess(false);
				result.add(atomR);
				LOG.debug("<recoveryDirs> sn == null snName :{}",snName);
				continue;
			}
			snId = sn.getId();
			snSId = sim.getSecondServerID(snId);
			parser = new SecondIDParser(curatorClient, snId, baseRoutesPath);
			parser.updateRoute();
			errors = recoveryFiles(sm, sim, parser, sn, atom,dataPath);
			if(errors == null || errors.isEmpty()){
				result.add(atomR);
				LOG.debug("<recoveryDirs> result is empty snName:{}", snName);
				continue;
			}
			atomR.addAll(errors);
			atomR.setSuccess(false);
			result.setSuccess(false);
		}
		return result;
	}
	/**
	 * 概述：字符串转Batch
	 * @param content
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static BatchAtomModel converStringToBatch(String content){
		if (BrStringUtils.isEmpty(content)) {
			LOG.warn("content is empty");
			return null;
		}
		BatchAtomModel batch = JsonUtils.toObject(content, BatchAtomModel.class);
		if (batch == null) {
			LOG.warn("batch content is empty");
			return null;
		}
		return batch;
	}

	/**
	 * 概述：修复文件
	 * @param sm
	 * @param snm
	 * @param sim
	 * @param atom
	 * @param parser
	 * @param client
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> recoveryFiles(ServiceManager sm,ServerIDManager sim, SecondIDParser parser, StorageNameNode snNode,AtomTaskModel atom, String dataPath) {

		String snName = atom.getStorageName();
		String dirName = atom.getDirName();
		List<String> fileNames = atom.getFiles();
		if (fileNames == null || fileNames.isEmpty()) {
			LOG.debug("<recoverFiles> {} files name is empyt", snName);
			return null;
		}
		if (snNode == null) {
			LOG.debug("<recoverFiles> {} sn node is empty", snName);
			return null;
		}
		boolean isSuccess = false;
		List<String> errors = new ArrayList<String>();
		for (String fileName : fileNames) {
			isSuccess = recoveryFileByName( sm, sim, parser, snNode, fileName, dirName, dataPath);
			if(!isSuccess){
				errors.add(fileName);
			}
		}
		return errors;
	}
	/**
	 * 概述：恢复单个文件
	 * @param client
	 * @param sm
	 * @param sim
	 * @param parser
	 * @param snNode
	 * @param fileName
	 * @param dirName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean recoveryFileByName(ServiceManager sm,ServerIDManager sim, SecondIDParser parser, StorageNameNode snNode, String fileName,String dirName, String dataPath){
		String[] sss = null;
		String remoteName = null;
		Service remoteService = null;
		String path = null;
		String localPath = null;
		int remoteIndex = 0;
		int localIndex = 0;
		String remotePath = null;
		String serverId = sim.getFirstServerID();
		boolean isSuccess = false;
		String snName = snNode.getName();
		int snId = snNode.getId();
		sss = parser.getAliveSecondID(fileName);
		if (sss == null) {
			LOG.debug("<recoveryFile> alive second Ids is empty");
			return false;
		}
		String secondId = sim.getSecondServerID(snId);
		if (BrStringUtils.isEmpty(secondId)) {
			LOG.debug("<recoveryFile> {} {} secondid is empty ",snName, snId);
			return false;
		}
		localIndex = isContain(sss, secondId);
		if (-1 == localIndex) {
			LOG.debug("<recoveryFile> {} {} {} is not mine !! skip",secondId, snName, fileName );
			return false;
		}
		
		localPath = "/"+snName + "/" + localIndex + "/" + dirName + "/" + fileName;
		String localDir = "/"+snName + "/" + localIndex + "/" + dirName+"/";
		File dir = new File(dataPath + localDir);
		if(!dir.exists()) {
			boolean createFlag = dir.mkdirs();
			LOG.debug("<recoveryFile> create dir :{}, stat:{}",localDir,createFlag);
		}
		
		File file = new File(dataPath + localPath);
		if(file.exists()){
			LOG.debug("<recoveryFile> {} {} is exists, skip",snName, fileName);
			return true;
		}
		remoteIndex = 0;
		for (String snsid : sss) {
			remoteIndex ++;
			//排除自己
			if (secondId.equals(snsid)) {
				LOG.debug("<recoveryFile> my sum is right,not need to do {} {} {}",fileName, secondId,snsid);
				continue;
			}
			
			remoteName = sim.getOtherFirstID(snsid, snId);
			if(BrStringUtils.isEmpty(remoteName)){
				LOG.debug("<recoveryFile> remote name is empty");
				continue;
			}
			remoteService = sm.getServiceById(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, remoteName);
			if(remoteService == null){
				LOG.debug("<recoveryFile> remote service is empty");
				continue;
			}
			remotePath = "/"+snName + "/" + remoteIndex + "/" + dirName + "/" + fileName;
			isSuccess = recoveryFile(remoteService, dataPath + localPath, remotePath);
			LOG.debug("<recoveryFile> recovery file sn:{},localsnId {}, remoteIndex:{}, fileName :{},stat {}", snName,snsid,remoteIndex,fileName,isSuccess);
			if(isSuccess){
				return true;
			}
		}
		return false;
	}
	/***
	 * 概述：批量恢复任务
	 * @param service
	 * @param paths
	 * @param count
	 * @param sleepTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean recoveryFile(Service service, String localPath, String remotePath) {
		// 文件恢复线程
		DiskNodeClient client = new LocalDiskNodeClient();
		boolean isSuccess = false;
		try {
			client.copyFrom(service.getHost(), service.getPort(), remotePath, localPath);
			isSuccess =  true;
		}
		catch (Exception e) {
			e.printStackTrace();
			isSuccess =  false;
		}finally {
			if(client != null){
				try {
					client.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			LOG.info("remote address {}:{}, remote {}, local {}, stat {}",service.getHost(),service.getPort(), remotePath, localPath,isSuccess ? "success" :"fail");
			return isSuccess;
		}

	}

	/**
	 * 概述：判断serverID是否存在
	 * @param context
	 * @param second
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static int isContain(String[] context, String second) {
		if (context == null || context.length == 0 || BrStringUtils.isEmpty(second)) {
			return -1;
		}
		int i = 0;
		for (String str : context) {
			i++;
			if (BrStringUtils.isEmpty(str)) {
				continue;
			}
			if (second.equals(str)) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * 概述：获取文件列表
	 * @param fileName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> getSNIds(String fileName) {
		if (BrStringUtils.isEmpty(fileName)) {
			return null;
		}

		String[] tmp = BrStringUtils.getSplit(fileName, "_");
		if (tmp == null || tmp.length == 0) {
			return null;
		}
		List<String> snIds = new ArrayList<String>();
		for (int i = 1; i < tmp.length; i++) {
			snIds.add(tmp[i]);
		}
		return snIds;
	}
}
