package com.bonree.brfs.disknode.server.handler;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BitSetUtils;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.client.AvailableSequenceInfo;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.disknode.client.RecoverInfo;
import com.bonree.brfs.disknode.data.write.BufferedFileWriter;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.buf.ByteArrayFileBuffer;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.data.write.record.RecordElementReader;
import com.bonree.brfs.disknode.data.write.record.RecordFileBuilder;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.utils.Pair;

public class RecoveryMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(RecoveryMessageHandler.class);
	
	private DiskContext context;
	private ServiceManager serviceManager;
	private FileWriterManager writerManager;
	private RecordCollectionManager recorderManager;
	
	public RecoveryMessageHandler(DiskContext context,
			ServiceManager serviceManager,
			FileWriterManager writerManager,
			RecordCollectionManager recorderManager) {
		this.context = context;
		this.serviceManager = serviceManager;
		this.writerManager = writerManager;
		this.recorderManager = recorderManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult handleResult = new HandleResult();
		String filePath = null;
		try {
			filePath = context.getConcreteFilePath(msg.getPath());
			LOG.info("starting recover file[{}]", filePath);
			
			RecoverInfo info = ProtoStuffUtils.deserialize(msg.getContent(), RecoverInfo.class);
			List<AvailableSequenceInfo> seqInfos = info.getInfoList();
			BitSet lack = new BitSet();
			lack.set(0, info.getMaxSeq() + 1);
			
			LOG.info("excepted max seq is {}, seq list size->{}", info.getMaxSeq(), seqInfos.size());
			SortedMap<Integer, byte[]> datas = new TreeMap<Integer, byte[]>();
			
			RandomAccessFile originFile = null;
			RecordElementReader recordreReader = null;
			try {
				Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(filePath, false);
				LOG.info("get binding for file[{}]-->{}", filePath, binding);
				if(binding != null) {
					binding.first().flush();
					RecordCollection recordSet = binding.first().getRecordCollection();
					originFile = new RandomAccessFile(filePath, "r");
					MappedByteBuffer buf = originFile.getChannel().map(MapMode.READ_ONLY, 0, originFile.length());
					
					recordreReader = recordSet.getRecordElementReader();
					for(RecordElement element : recordreReader) {
						byte[] bytes = new byte[element.getSize()];
						buf.position((int) element.getOffset());
						buf.get(bytes);
						
						datas.put(element.getSequence(), bytes);
						lack.set(element.getSequence(), false);
					}
				}
			} catch (Exception e) {
				LOG.error("search datas at original file[{}] error", filePath, e);
			} finally {
				CloseUtils.closeQuietly(recordreReader);
			}
			
			LOG.info("starting... file[{}] lack seq number-->{}", filePath, lack.cardinality());
			
			try {
				for(AvailableSequenceInfo seqInfo : seqInfos) {
					if(lack.cardinality() ==  0) {
						break;
					}
					
					BitSet seqSet = seqInfo.getAvailableSequence();
					LOG.info("this loop available size{}, lack size{}", seqSet.cardinality(), lack.cardinality());
					BitSet availableSeq = BitSetUtils.intersect(seqSet, lack);
					if(availableSeq.cardinality() != 0) {
						Service service = serviceManager.getServiceById(seqInfo.getServiceGroup(), seqInfo.getServiceId());
						if(service == null) {
							LOG.error("can not get service with[{}:{}]", seqInfo.getServiceGroup(), seqInfo.getServiceId());
							continue;
						}
						
						DiskNodeClient client = null;
						try {
							LOG.info("get data from{} to recover...", service);
							client = new HttpDiskNodeClient(service.getHost(), service.getPort());
							
							for(int i = availableSeq.nextSetBit(0); i != -1; i = availableSeq.nextSetBit(++i)) {
								byte[] bytes = client.getBytesBySequence(seqInfo.getFilePath(), i);
								if(bytes == null) {
									break;
								}
								
								lack.set(i, false);
								datas.put(i, bytes);
							}
						} catch (Exception e) {
							LOG.error("recover file[{}] error", filePath, e);
						} finally {
							CloseUtils.closeQuietly(client);
						}
						
					}
				}
			} catch (Exception e) {
				LOG.error("recovery file[{}] error!", filePath, e);
			} finally {
				CloseUtils.closeQuietly(originFile);
			}
			
			LOG.info("finally file[{}] lack size = {}", filePath, lack.cardinality());
			
			if(lack.cardinality() != 0) {
				handleResult.setSuccess(false);
				handleResult.setCause(new Exception("can not get all data to recover file!"));
				return;
			}
			
			DataFileRewriter rewriter = new DataFileRewriter(filePath, datas);
			rewriter.rewrite();
			
			handleResult.setSuccess(rewriter.success());
		} catch (Exception e) {
			LOG.error("recover file[{}] error", filePath, e);
			handleResult.setSuccess(false);
		} finally {
			callback.completed(handleResult);
		}
	}
	
	private class DataFileRewriter {
		private static final String REWRITE_SUFFIX = "_rewrite";
		
		private String filePath;
		private SortedMap<Integer, byte[]> datas;
		
		private boolean completed = false;
		
		public DataFileRewriter(String filePath, SortedMap<Integer, byte[]> datas) {
			this.filePath = filePath;
			this.datas = datas;
		}
		
		public boolean success() {
			return completed;
		}
		
		private File buildRewriteFile(String filePath) {
			StringBuilder builder = new StringBuilder();
			builder.append(filePath).append(REWRITE_SUFFIX);
			
			return new File(builder.toString());
		}
		
		public void rewrite() {
			File rewriteFile = buildRewriteFile(filePath);
			File rewriteFileRd = RecordFileBuilder.buildFrom(rewriteFile);
			File originFile = new File(filePath);
			File originFileRd = RecordFileBuilder.buildFrom(originFile);
			
			try {
				RecordFileWriter writer = new RecordFileWriter(
						recorderManager.getRecordCollection(rewriteFile, false, 8196, false),
						new BufferedFileWriter(rewriteFile, new ByteArrayFileBuffer(1024 * 1024)));
				
				for(Entry<Integer, byte[]> entry : datas.entrySet()) {
					writer.updateSequence(entry.getKey());
					writer.write(entry.getValue());
				}
				
				writer.flush();
				writer.close();
				
				writerManager.close(filePath);
				originFile.delete();
				
				rewriteFile.renameTo(originFile);
				rewriteFileRd.renameTo(originFileRd);
				
				writerManager.rebuildFileWriter(originFile);
				
				LOG.info("rewrite file[{}] success", filePath);
				completed = true;
			} catch (IOException e) {
				LOG.error("rewrite file[{}] error", filePath, e);
			} finally {
				writerManager.close(rewriteFile.getAbsolutePath());
				rewriteFile.delete();
			}
		}
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}
	
}
