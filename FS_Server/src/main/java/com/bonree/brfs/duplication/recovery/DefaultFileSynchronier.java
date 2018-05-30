package com.bonree.brfs.duplication.recovery;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BitSetUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.disknode.client.AvailableSequenceInfo;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.RecoverInfo;
import com.bonree.brfs.duplication.DuplicationEnvironment;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FileNodeStorer;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.server.identification.ServerIDManager;

public class DefaultFileSynchronier implements FileSynchronizer {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileSynchronier.class);
	
	private static final int DEFAULT_THREAD_NUM = 1;
	private ExecutorService threadPool;
	
	private DiskNodeConnectionPool connectionPool;
	private FileNodeStorer recoveryStorer;
	
	private ServerIDManager idManager;
	
	public DefaultFileSynchronier(DiskNodeConnectionPool connectionPool, FileNodeStorer recoveryStorer, ServerIDManager idManager) {
		this(DEFAULT_THREAD_NUM, connectionPool, recoveryStorer, idManager);
	}
	
	public DefaultFileSynchronier(int threadNum, DiskNodeConnectionPool connectionPool, FileNodeStorer recoveryStorer, ServerIDManager idManager) {
		this.connectionPool = connectionPool;
		this.recoveryStorer = recoveryStorer;
		this.idManager = idManager;
		this.threadPool = new ThreadPoolExecutor(threadNum, threadNum,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new PooledThreadFactory("file_recovery"));
	}

	@Override
	public void start() throws Exception {
	}

	@Override
	public void stop() throws Exception {
		threadPool.shutdown();
	}

	@Override
	public void recover(FileNode fileNode, FileRecoveryListener listener) {
		threadPool.submit(new RecoveryTask(fileNode, listener));
	}
	
	public class RecoveryTask implements Runnable {
		private FileNode target;
		private FileRecoveryListener listener;
		
		public RecoveryTask(FileNode fileNode, FileRecoveryListener listener) {
			this.target = fileNode;
			this.listener = listener;
		}

		@Override
		public void run() {
			LOG.info("start recovery file[{}]", target.getName());
			
			/**
			 * 文件之间的内容协调是通过写入文件的序列号实现的，只要当前存活的磁盘节点包含
			 * 所有写入序列号就能保证文件的完整性
			 */
			List<DuplicateNodeSequence> seqNumberList = getAllDuplicateNodeSequence();
			
			if(seqNumberList.size() != target.getDuplicateNodes().length) {
				//不相等的情况不能再继续恢复了，我们选择等待
				
			}
			
			if(seqNumberList.isEmpty()) {
				listener.error(new Exception("No available duplicate node to found for file[" + target.getName() + "]"));
				return;
			}
			
			BitSet[] sets = new BitSet[seqNumberList.size()];
			for(int i = 0; i < sets.length; i++) {
				sets[i] = seqNumberList.get(i).getSequenceNumbers();
			}
			
			//每个副本节点的序列号的交集
			BitSet union = BitSetUtils.union(sets);
			//每个副本节点都有的序列号
			BitSet intersection = BitSetUtils.intersect(sets);
			
			/**
			 * 查看所有节点的序列号是否覆盖了[0, maxSeq]之间的所有数值
			 */
			LOG.info("Recovery Check report union[{}], itersection[{}]", union.cardinality(), intersection.cardinality());
			if(union.nextSetBit(union.cardinality()) == -1) {
				//当前存活的所有节点包含了此文件的所有信息，可以进行文件内容同步
				
				if(seqNumberList.size() < target.getDuplicateNodes().length) {
					//到这说明虽然在有部分副本信息没获取成功的前提下，文件的序列号还是连续的，这种情况下，虽然可以对文件进行修补，
					//但并不能保证文件是完整的，只能保证[0, maxSeq]之间的数据是正确可用的
					LOG.warn("File sequence numbers of [{}] can not be guaranteed to be full informed, although it seems to be perfect.", target.getName());
					
				}
				
				if(intersection.cardinality() == union.cardinality()) {
					//如果交集和并集的数量一样，说明没有文件数据缺失
					LOG.info("file[{}] is ok!", target.getName());
					listener.complete(target);
					return;
				}
				
				doRecover(seqNumberList, union, intersection);
			} else {
				//存在文件内容丢失，怎么办，怎么办
				//TODO 处理下这个情况
				listener.error(new Exception("Content of the file[" + target.getName() + "] is deficient!!"));
			}
			
			LOG.info("End recovery file[{}]", target.getName());
		}
		
		
		private List<DuplicateNodeSequence> getAllDuplicateNodeSequence() {
			List<DuplicateNodeSequence> seqNumberList = new ArrayList<DuplicateNodeSequence>();
			
			for(DuplicateNode node : target.getDuplicateNodes()) {
				if(node.getGroup().equals(DuplicationEnvironment.VIRTUAL_SERVICE_GROUP)) {
					continue;
				}
				
				DiskNodeConnection connection = connectionPool.getConnection(node);
				if(connection == null || connection.getClient() == null) {
					LOG.error("duplication node[{}, {}] of [{}] is not available, that's maybe a trouble!", node.getGroup(), node.getId(), target.getName());
					continue;
				}
				
				String serverId = idManager.getOtherSecondID(node.getId(), target.getStorageId());
				String filePath =FilePathBuilder.buildPath(target, serverId);
				LOG.info("checking---{}", filePath);
				BitSet seqNumbers = connection.getClient().getWritingSequence(filePath);
				
				if(seqNumbers == null || seqNumbers.cardinality() == 0) {
					LOG.error("duplication node[{}, {}] of [{}] can not get file sequences, that's maybe a trouble!", node.getGroup(), node.getId(), target.getName());
					continue;
				}
				
				LOG.info("server{} -- {}", serverId, seqNumbers.cardinality());
				
				DuplicateNodeSequence nodeSequence = new DuplicateNodeSequence();
				nodeSequence.setNode(node);
				nodeSequence.setSequenceNumbers(seqNumbers);
				
				seqNumberList.add(nodeSequence);
			}
			
			return seqNumberList;
		}
		
		private void doRecover(List<DuplicateNodeSequence> seqNumberList, BitSet union, BitSet intersection) {
			List<AvailableSequenceInfo> infos = new ArrayList<AvailableSequenceInfo>();
			for(DuplicateNodeSequence sequence : seqNumberList) {
				BitSet iHave = BitSetUtils.minus(sequence.getSequenceNumbers(), intersection);
				AvailableSequenceInfo info = new AvailableSequenceInfo();
				info.setServiceGroup(sequence.getNode().getGroup());
				info.setServiceId(sequence.getNode().getId());
				info.setAvailableSequence(iHave);
				
				String serverId = idManager.getOtherSecondID(sequence.getNode().getId(), target.getStorageId());
				info.setFilePath(FilePathBuilder.buildPath(target, serverId));
				
				infos.add(info);
			}
			
			//这部分我们称呼它为“补漏”
			RecoverInfo recoverInfo = new RecoverInfo();
			recoverInfo.setMaxSeq(union.cardinality() - 1);
			recoverInfo.setInfoList(infos);
			for(DuplicateNodeSequence sequence : seqNumberList) {
				BitSet lack = BitSetUtils.minus(union, sequence.getSequenceNumbers());
				if(lack.isEmpty()) {
					//没有空缺内容，不需要恢复
					continue;
				}
				
				DiskNodeConnection connection = connectionPool.getConnection(sequence.getNode());
				if(connection == null || connection.getClient() == null) {
					LOG.error("can not recover file[{}], because of lack of connection to duplication node[{}]", target.getName(), sequence.getNode());
					continue;
				}
				
				DiskNodeClient client = connection.getClient();
				String serverId = idManager.getOtherSecondID(sequence.getNode().getId(), target.getStorageId());
				LOG.info("start recover file[{}] at duplicate node[{}]", target.getName(), sequence.getNode());
				if(!client.recover(FilePathBuilder.buildPath(target, serverId), recoverInfo)) {
					listener.error(new Exception("can not recover file[" + target.getName() + "] at duplicate node" + sequence.getNode()));
					return;
				}
			}
			
			listener.complete(target);
		}
	}

}
