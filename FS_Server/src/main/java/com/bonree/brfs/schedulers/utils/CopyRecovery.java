
package com.bonree.brfs.schedulers.utils;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.bonree.brfs.rebalance.route.impl.RouteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.disknode.client.TcpDiskNodeClient;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.system.CopyCheckJob;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.AtomTaskResultModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.server.identification.ServerIDManager;
import com.bonree.mail.worker.MailWorker;

public class CopyRecovery {
	private static final Logger LOG = LoggerFactory.getLogger(CopyRecovery.class);
	/**
	 * 概述：修复目录
	 * @param content
	 * @param baseRoutesPath
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static TaskResultModel recoveryDirs(String content, String baseRoutesPath,String dataPath) {
		TaskResultModel result = new TaskResultModel();
		BatchAtomModel batch = converStringToBatch(content);
		if(batch == null){
			result.setSuccess(false);
			LOG.debug("batch is empty");
			return result;
		}
		List<AtomTaskModel> atoms = batch.getAtoms();
		if(atoms == null|| atoms.isEmpty()){
			result.setSuccess(true);
			LOG.debug(" files is empty");
			return result;
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		ServerIDManager sim = mcf.getSim();
		ServiceManager sm = mcf.getSm();
		StorageRegionManager snm = mcf.getSnm();
		
		CuratorClient curatorClient = mcf.getClient();
		StorageRegion sn;
		RouteParser parser;
		String snName;
		int snId;
		AtomTaskResultModel atomR;
		List<String> errors;
		for (AtomTaskModel atom : atoms) {
			atomR = new AtomTaskResultModel();
			atomR.setFiles(atom.getFiles());
			atomR.setSn(atom.getStorageName());
			snName = atom.getStorageName();
			sn = snm.findStorageRegionByName(snName);
			if (sn == null) {
				atomR.setSuccess(false);
				result.setSuccess(false);
				result.add(atomR);
				LOG.debug("sn == null snName :{}",snName);
				continue;
			}
			snId = sn.getId();
			parser = new RouteParser(snId, mcf.getRouteLoader());
			errors = recoveryFiles(sm, sim, parser, sn, atom,dataPath);
			if(errors == null || errors.isEmpty()){
				result.add(atomR);
				LOG.debug("result is empty snName:{}", snName);
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
		BatchAtomModel batch = JsonUtils.toObjectQuietly(content, BatchAtomModel.class);
		if (batch == null) {
			LOG.warn("batch content is empty");
			return null;
		}
		return batch;
	}

	/**
	 * 概述：修复文件
	 * @param sm
	 * @param sim
	 * @param atom
	 * @param parser
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> recoveryFiles(ServiceManager sm,ServerIDManager sim, RouteParser parser, StorageRegion snNode,AtomTaskModel atom, String dataPath) {

		String snName = atom.getStorageName();
		long start = TimeUtils.getMiles(atom.getDataStartTime(), TimeUtils.TIME_MILES_FORMATE);
		long endTime = TimeUtils.getMiles(atom.getDataStopTime(), TimeUtils.TIME_MILES_FORMATE);
		long granule = endTime - start;
		// 过滤过期的数据
		long ttl = Duration.parse(snNode.getDataTtl()).toMillis();
		long currentTime = System.currentTimeMillis();
		String dirName = TimeUtils.timeInterval(start, granule);
		if(currentTime - endTime > ttl){
			LOG.info("{}[ttl:{}ms] {} is expired !! skip repaired !!!",snName,ttl,dirName);
			return null;
		}
		List<String> fileNames = atom.getFiles();
		if (fileNames == null || fileNames.isEmpty()) {
			LOG.debug("{} files name is empty", snName);
			return null;
		}
		if (snNode == null) {
			LOG.debug(" {} sn node is empty", snName);
			return null;
		}
		boolean isSuccess;
		List<String> errors = new ArrayList<>();
		for (String fileName : fileNames) {
			isSuccess = recoveryFileByName( sm, sim, parser, snNode, fileName, dirName, dataPath,atom.getTaskOperation());
			if(!isSuccess){
				errors.add(fileName);
			}
		}
		return errors;
	}
	/**
	 * 概述：恢复单个文件
	 * @param sm
	 * @param sim
	 * @param parser
	 * @param snNode
	 * @param fileName
	 * @param dirName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean recoveryFileByName(ServiceManager sm,ServerIDManager sim, RouteParser parser, StorageRegion snNode, String fileName,String dirName, String dataPath,String operation){
		String[] sss;
		String remoteName;
		Service remoteService;
		String localPath;
		int remoteIndex;
		int localIndex;
		String remotePath;
		boolean isSuccess = true;
		String snName = snNode.getName();
		int snId = snNode.getId();
		sss = parser.searchVaildIds(fileName);
		if (sss == null) {
			LOG.warn("alive second Ids is empty");
			return false;
		}
		String secondId = sim.getSecondServerID(snId);
		if (BrStringUtils.isEmpty(secondId)) {
			LOG.warn("{} {} secondid is empty ",snName, snId);
			return false;
		}
		localIndex = isContain(sss, secondId);
		if (-1 == localIndex) {
			LOG.info("{} {} {} is not mine !! skip",secondId, snName, fileName );
			return true;
		}
		
		localPath = "/"+snName + "/" + localIndex + "/" + dirName + "/" + fileName;
		String localDir = "/"+snName + "/" + localIndex + "/" + dirName+"/";
		File dir = new File(dataPath + localDir);
		if(!dir.exists()) {
			boolean createFlag = dir.mkdirs();
			LOG.debug("create dir :{}, stat:{}",localDir,createFlag);
		}
		if(CopyCheckJob.RECOVERY_CRC.equals(operation)) {
			boolean flag = FileCollection.check(dataPath + localPath);
			LOG.debug("locaPath : {}, CRCSTATUS: {}", dataPath+localPath, flag);
			if(flag) {
				return true;
			}else {
				boolean status = FileUtils.deleteFile(dataPath+localPath);
				LOG.warn("{} crc is error!! delete {}", localPath,status);
			}
		}else {
			File file = new File(dataPath + localPath);
			if(file.exists()){
				LOG.debug("{} {} is exists, skip",snName, fileName);
				return true;
			}
		}
		remoteIndex = 0;
		for (String snsid : sss) {
			remoteIndex ++;
			//排除自己
			if (secondId.equals(snsid)) {
				LOG.debug(" my son is right,not need to do {} {} {}",fileName, secondId,snsid);
				continue;
			}
			
			remoteName = sim.getOtherFirstID(snsid, snId);
			if(BrStringUtils.isEmpty(remoteName)){
				LOG.warn("remote name is empty");
				continue;
			}
			remoteService = sm.getServiceById(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME), remoteName);
			if(remoteService == null){
				LOG.warn("remote service is empty");
				continue;
			}
			remotePath = "/"+snName + "/" + remoteIndex + "/" + dirName + "/" + fileName;
			isSuccess = copyFrom(remoteService.getHost(), remoteService.getPort(),remoteService.getExtraPort(),5000, remotePath, dataPath + localPath);
			LOG.info("remote address [{}: {} ：{}], remote [{}], local [{}], stat [{}]",
				remoteService.getHost(), remoteService.getPort(), remoteService.getExtraPort(), 
				remotePath, localPath, isSuccess ? "success" :"fail");
			if(isSuccess){
				return true;
			}
		}
		return isSuccess;
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
	 * 概述：恢复数据文件
	 * @param host 远程主机
	 * @param port 端口
	 * @param export
	 * @param timeout
	 * @param remotePath
	 * @param localPath
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean copyFrom(String host, int port,int export,int timeout, String remotePath, String localPath) {
		TcpDiskNodeClient client = null;
		try {
			client = TcpClientUtils.getClient(host, port, export, timeout);
			LOG.debug("{}:{},{}:{}, read {} to local {}",host,port,host,export,remotePath,localPath);
			LocalByteStreamConsumer consumer = new LocalByteStreamConsumer(localPath);
			client.readFile(remotePath, consumer);
			return consumer.getResult().get();
		}catch (InterruptedException | IOException | ExecutionException e) {
			EmailPool emailPool = EmailPool.getInstance();
			MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
			builder.setModel("collect file execute 模块服务发生问题");
			builder.setException(e);
			ManagerContralFactory mcf = ManagerContralFactory.getInstance();
			builder.setMessage(mcf.getGroupName()+"("+mcf.getServerId()+")服务 执行任务时发生问题");
			Map<String,String> map = new HashMap<>();
			map.put("remote ",host);
			map.put("remote path",remotePath);
			map.put("local path", localPath);
			map.put("connectTimeout", String.valueOf(timeout));
			builder.setVariable(map);
			emailPool.sendEmail(builder);
			LOG.error("copy from error {}",e);
			return false;
		} finally {
			if (client != null) {
				try {
					client.closeFile(remotePath);
					client.close();
				}
				catch (IOException e) {
					LOG.error("close error ", e);
				}
			}
			
		}
	}
}
