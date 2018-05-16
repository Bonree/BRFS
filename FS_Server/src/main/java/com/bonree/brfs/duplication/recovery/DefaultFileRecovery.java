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
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.SeqInfo;
import com.bonree.brfs.disknode.client.SeqInfoList;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.server.identification.ServerIDManager;

public class DefaultFileRecovery implements FileRecovery {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultFileRecovery.class);
	
	private static final int DEFAULT_THREAD_NUM = 1;
	private ExecutorService threadPool;
	
	private DiskNodeConnectionPool connectionPool;
	
	private ServerIDManager idManager;
	
	public DefaultFileRecovery(DiskNodeConnectionPool connectionPool, ServerIDManager idManager) {
		this(DEFAULT_THREAD_NUM, connectionPool, idManager);
	}
	
	public DefaultFileRecovery(int threadNum, DiskNodeConnectionPool connectionPool, ServerIDManager idManager) {
		this.connectionPool = connectionPool;
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
			/**
			 * 文件之间的内容协调是通过写入文件的序列号实现的，只要当前存活的磁盘节点包含
			 * 所有写入序列号就能保证文件的完整性
			 */
			DuplicateNode[] duplicates = target.getDuplicateNodes();
			
			List<FileSequence> seqList = new ArrayList<FileSequence>();
			
			for(int i = 0; i < duplicates.length; i++) {
				DiskNodeConnection connection = connectionPool.getConnection(duplicates[i]);
				if(connection == null || connection.getClient() == null) {
					continue;
				}
				
				DiskNodeClient client = connection.getClient();
				
				String serverId = idManager.getOtherSecondID(duplicates[i].getId(), target.getStorageId());
				BitSet seqs = client.getWritingSequence(FilePathBuilder.buildPath(target, serverId));
				
				if(seqs == null) {
					LOG.info("server{} -- null", serverId);
					continue;
				}
				
				LOG.info("server{} -- {}", serverId, seqs.cardinality());
				
				FileSequence seq = new FileSequence();
				seq.setNode(duplicates[i]);
				seq.setSequenceSet(seqs);
				
				seqList.add(seq);
			}
			
			BitSet[] sets = new BitSet[seqList.size()];
			for(int i = 0; i < sets.length; i++) {
				sets[i] = seqList.get(i).getSequenceSet();
			}
			
			BitSet union = BitSetUtils.union(sets);
			BitSet intersection = BitSetUtils.intersect(sets);
			
			/**
			 * 查看所有节点的序列号是否覆盖了[0, maxSeq]之间的所有数值
			 */
			LOG.info("Recovery Check union[{}]", union.cardinality());
			if(union.nextSetBit(union.cardinality()) == -1) {
				//当前存活的所有节点包含了此文件的所有信息，可以进行文件内容同步
				
				if(intersection.cardinality() == union.cardinality()) {
					//如果交集和并集的数量一样，说明没有文件数据缺失
					listener.complete(target);
					return;
				}
				
				List<SeqInfo> infos = new ArrayList<SeqInfo>();
				for(FileSequence sequence : seqList) {
					BitSet set = sequence.getSequenceSet();
					BitSet iHave = BitSetUtils.minus(set, intersection);
					
					SeqInfo info = new SeqInfo();
					
					DiskNodeConnection connection = connectionPool.getConnection(sequence.getNode());
					if(connection == null || connection.getClient() == null) {
						continue;
					}
					
					info.setHost(connection.getService().getHost());
					info.setPort(connection.getService().getPort());
					info.setIntArray(iHave);
					infos.add(info);
				}
				
				for(FileSequence sequence : seqList) {
					BitSet lack = BitSetUtils.minus(union, sequence.getSequenceSet());
					if(lack.isEmpty()) {
						continue;
					}
					
					SeqInfoList infoList = new SeqInfoList();
					
					List<SeqInfo> complements = new ArrayList<SeqInfo>();
					for(SeqInfo seqInfo : infos) {
						BitSet set = BitSetUtils.intersect(new BitSet[]{seqInfo.getIntArray(), lack});
						if(set.isEmpty()) {
							continue;
						}
						
						lack.andNot(set);
						
						SeqInfo complement = new SeqInfo();
						complement.setIntArray(set);
						complement.setHost(seqInfo.getHost());
						complement.setPort(seqInfo.getPort());
						
						complements.add(complement);
					}
					
					infoList.setInfoList(complements);
					
					DiskNodeConnection connection = connectionPool.getConnection(sequence.getNode());
					if(connection == null || connection.getClient() == null) {
						continue;
					}
					
					DiskNodeClient client = connection.getClient();
					try {
						String serverId = idManager.getOtherSecondID(sequence.getNode().getId(), target.getStorageId());
						client.recover(FilePathBuilder.buildPath(target, serverId), infoList);
					} catch (Exception e) {
						e.printStackTrace();
					}
					listener.complete(target);
				}
			} else {
				//存在文件内容丢失，怎么办，怎么办
				//TODO 处理下这个情况
				listener.error(new Exception("Content of the file[" + target.getName() + "] is deficient!!"));
			}
		}
		
	}

}
