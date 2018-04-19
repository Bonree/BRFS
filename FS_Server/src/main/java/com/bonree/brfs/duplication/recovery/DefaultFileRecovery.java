package com.bonree.brfs.duplication.recovery;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Op.Check;

import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.FileNodeProfile;
import com.bonree.brfs.disknode.record.RecordElement;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.coordinator.FileNameBuilder;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.coordinator.FilePathBuilder;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;

public class DefaultFileRecovery implements FileRecovery {
	private static final int DEFAULT_THREAD_NUM = 1;
	private ExecutorService threadPool;
	
	private DiskNodeConnectionPool connectionPool;
	
	public DefaultFileRecovery(DiskNodeConnectionPool connectionPool) {
		this(DEFAULT_THREAD_NUM, connectionPool);
	}
	
	public DefaultFileRecovery(int threadNum, DiskNodeConnectionPool connectionPool) {
		this.connectionPool = connectionPool;
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
		
		private void check() {
			DuplicateNode[] duplicates = target.getDuplicateNodes();
			
			List<FileSequence> seqList = new ArrayList<FileSequence>();
			for(int i = 0; i < duplicates.length; i++) {
				DiskNodeConnection connection = connectionPool.getConnection(duplicates[i]);
				if(connection == null || connection.getClient() == null) {
					continue;
				}
				
				DiskNodeClient client = connection.getClient();
				BitSet seqs = client.getWritingSequence(FilePathBuilder.buildPath(target));
				if(seqs == null) {
					continue;
				}
				
				FileSequence seq = new FileSequence();
				seq.setNode(duplicates[i]);
				seq.setSequenceSet(seqs);
			}
			
			int maxSeq = target.getWriteSequence();
			BitSet fullSet = new BitSet(maxSeq);
			/**
			 * 查看所有节点的序列号是否覆盖了[0, maxSeq]之间的所有数值
			 */
			boolean full = false;
			for(FileSequence sequence : seqList) {
				fullSet.or(sequence.getSequenceSet());
				if(fullSet.cardinality() == maxSeq) {
					full = true;
					break;
				}
			}
			
			if(full) {
				//当前存活的所有节点包含了此文件的所有信息，可以进行文件内容同步
				for(FileSequence sequence : seqList) {
					BitSet set = sequence.getSequenceSet();
					set.flip(0, maxSeq);
				}
			} else {
				//存在文件内容丢失，怎么办，怎么办
				//TODO 处理下这个情况
			}
		}

		@Override
		public void run() {
			/**
			 * 文件之间的内容协调是通过写入文件的序列号实现的，只要当前存活的磁盘节点包含
			 * 所有写入序列号就能保证文件的完整性
			 */
			
			DuplicateNode[] duplicates = target.getDuplicateNodes();
			FileNodeProfile[] profiles = new FileNodeProfile[duplicates.length];
			
			List<DuplicateNode> needRecover = new ArrayList<DuplicateNode>();
			FileNodeProfile maxValid = null;
			DuplicateNode from = null;
			for(int i = 0; i < duplicates.length; i++) {
				DiskNodeConnection connection = connectionPool.getConnection(duplicates[i]);
				if(connection == null) {
					needRecover.add(duplicates[i]);
					continue;
				}
				
				DiskNodeClient client = connection.getClient();
				if(client == null) {
					needRecover.add(duplicates[i]);
					continue;
				}
				
//				profiles[i] = client.getFileNodeProfile(FilePathBuilder.buildPath(target));
//				if(profiles[i] == null) {
//					needRecover.add(duplicates[i]);
//					continue;
//				}
//				
//				if(maxValid == null || maxValid.getElements().length < profiles[i].getElements().length) {
//					maxValid = profiles[i];
//					from = duplicates[i];
//				}
			}
			
			if(maxValid == null) {
				listener.error(new Exception("What's fucking up of the file!?"));
				return;
			}
			
			for(int i = 0; i < duplicates.length; i++) {
				if(profiles[i] == null) {
					continue;
				}
				
				if(maxValid.getElements().length != profiles[i].getElements().length) {
					needRecover.add(duplicates[i]);
				}
			}
			
			recoverDuplicates(target, from, needRecover);
		}
		
		private void recoverDuplicates(FileNode file, DuplicateNode from, List<DuplicateNode> to) {
			//TODO 复制文件
		}
		
		private boolean isEqual(FileNodeProfile profile1, FileNodeProfile profile2) {
			RecordElement[] elements1 = profile1.getElements();
			RecordElement[] elements2 = profile2.getElements();
			
			if(elements1.length != elements2.length) {
				return false;
			}
			
			for(int i = 0; i < elements1.length; i++) {
				if(!elements1[i].equals(elements2[i])) {
					return false;
				}
			}
			
			return true;
		}
		
	}

}
