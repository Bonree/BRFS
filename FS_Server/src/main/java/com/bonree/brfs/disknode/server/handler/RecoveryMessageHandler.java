package com.bonree.brfs.disknode.server.handler;

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

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
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
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.utils.Pair;

public class RecoveryMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(RecoveryMessageHandler.class);
	
	private DiskContext context;
	private ServiceManager serviceManager;
	private FileWriterManager writerManager;
	
	public RecoveryMessageHandler(DiskContext context, ServiceManager serviceManager, FileWriterManager writerManager) {
		this.context = context;
		this.serviceManager = serviceManager;
		this.writerManager = writerManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		String filePath = context.getConcreteFilePath(msg.getPath());
		LOG.info("starting recover file[{}]", filePath);
		
		RecoverInfo info = ProtoStuffUtils.deserialize(msg.getContent(), RecoverInfo.class);
		List<AvailableSequenceInfo> seqInfos = info.getInfoList();
		BitSet lack = new BitSet();
		lack.set(0, info.getMaxSeq() + 1);
		
		LOG.info("excepted max seq is {}", info.getMaxSeq());
		SortedMap<Integer, byte[]> datas = new TreeMap<Integer, byte[]>();
		RecordCollection recordSet = null;
		RandomAccessFile originFile = null;
		
		try {
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(filePath, false);
			if(binding != null) {
				binding.first().flush();
				recordSet = binding.first().getRecordCollection();
				originFile = new RandomAccessFile(filePath, "r");
				MappedByteBuffer buf = originFile.getChannel().map(MapMode.READ_ONLY, 0, originFile.length());
				
				for(RecordElement element : recordSet) {
					byte[] bytes = new byte[element.getSize()];
					buf.position((int) element.getOffset());
					buf.get(bytes);
					
					datas.put(element.getSequence(), bytes);
					lack.set(element.getSequence(), false);
				}
				
				LOG.info("lack seq number-->{}", lack.cardinality());
				
				for(AvailableSequenceInfo seqInfo : seqInfos) {
					if(lack.cardinality() ==  0) {
						break;
					}
					
					LOG.info("this loop lack size => {}", lack.cardinality());
					BitSet availableSeq = BitSetUtils.intersect(seqInfo.getAvailableSequence(), lack);
					if(availableSeq.cardinality() != 0) {
						Service service = serviceManager.getServiceById(seqInfo.getServiceGroup(), seqInfo.getServiceId());
						
						DiskNodeClient client = null;
						try {
							client = new HttpDiskNodeClient(service.getHost(), service.getPort());
							
							for(int i = availableSeq.nextSetBit(0); i != -1; i = availableSeq.nextSetBit(++i)) {
								byte[] bytes = client.getBytesBySequence(seqInfo.getFilePath(), i);
								if(bytes != null) {
									lack.set(i, false);
									datas.put(i, bytes);
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							CloseUtils.closeQuietly(client);
						}
						
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(originFile);
			CloseUtils.closeQuietly(recordSet);
		}
		
		LOG.info("finally lack size = {}", lack.cardinality());
		
		if(lack.cardinality() != 0) {
			HandleResult handleResult = new HandleResult();
			handleResult.setSuccess(false);
			handleResult.setCause(new Exception("can not get all data to recover file!"));
			callback.completed(handleResult);
			return;
		}
		
		boolean writeOk = writeDatas(filePath, datas);
		HandleResult handleResult = new HandleResult();
		handleResult.setSuccess(writeOk);
		callback.completed(handleResult);
	}
	
	private boolean writeDatas(String path, SortedMap<Integer, byte[]> datas) {
		try {
			writerManager.close(path);
			
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(path, true);
			if(binding == null) {
				return false;
			}
			
			RecordFileWriter writer = binding.first();
			
			for(Entry<Integer, byte[]> entry : datas.entrySet()) {
				writer.updateSequence(entry.getKey());
				writer.write(entry.getValue());
			}
			
			writer.flush();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	@Override
	public boolean isValidRequest(HttpMessage message) {
		return true;
	}
	
}
