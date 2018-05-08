package com.bonree.brfs.disknode.server.handler;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntConsumer;

import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.HttpMessage;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.disknode.client.SeqInfo;
import com.bonree.brfs.disknode.client.SeqInfoList;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordCollectionManager;
import com.bonree.brfs.disknode.data.write.record.RecordElement;

public class RecoveryMessageHandler implements MessageHandler {
	private DiskContext context;
	private RecordCollectionManager recordManager;
	
	public RecoveryMessageHandler(DiskContext context, RecordCollectionManager recordManager) {
		this.context = context;
		this.recordManager = recordManager;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		String filePath = context.getConcreteFilePath(msg.getPath());
		
		SeqInfoList info = ProtoStuffUtils.deserialize(msg.getContent(), SeqInfoList.class);
		List<SeqInfo> seqInfos = info.getInfoList();
		
		Map<Integer, byte[]> datas = new HashMap<Integer, byte[]>();
		BitSet lack = new BitSet();
		for(SeqInfo seqInfo : seqInfos) {
			DiskNodeClient client = new HttpDiskNodeClient(seqInfo.getHost(), seqInfo.getPort());
			
			seqInfo.getIntArray().stream().forEach(new IntConsumer() {
				
				@Override
				public void accept(int value) {
					lack.set(value);
					byte[] bytes = client.getBytesBySequence(filePath, value);
					datas.put(value, bytes == null ? new byte[0] : bytes);
				}
				
			});
			
			CloseUtils.closeQuietly(client);
		}
		
		mergeFileShards(filePath, datas, lack);
	}

	private void mergeFileShards(String path, Map<Integer, byte[]> datas, BitSet lack) {
		RecordCollection recordSet = recordManager.getRecordCollectionReadOnly(path);
		
		RandomAccessFile rawFile = null;
		BufferedOutputStream newFileInput = null;
		try {
			rawFile = new RandomAccessFile(new File(path), "r");
			newFileInput = new BufferedOutputStream(new FileOutputStream(path + "_new"));
			
			int currentIndex = 0;
			long localOffset = 0;
			int localLength = 0;
			Iterator<RecordElement> iterator = recordSet.iterator();
			RecordElement lastElement = iterator.next();
			while(true) {
				byte[] bytes = datas.get(currentIndex);
				if(bytes != null) {
					newFileInput.write(bytes);
					
					currentIndex++;
					continue;
				}
				
				if(lastElement != null && lastElement.getSequence() == currentIndex) {
					localOffset = lastElement.getOffset();
					localLength = lastElement.getSize();
					currentIndex++;
				}
				
				while(true) {
					lastElement = iterator.next();
					if(lastElement.getSequence() != currentIndex) {
						break;
					}
					
					currentIndex++;
					localLength += lastElement.getSize();
				}
				
				if(localLength != 0) {
					byte[] rawBytes = new byte[localLength];
					rawFile.seek(localOffset);
					rawFile.readFully(rawBytes);
					
					newFileInput.write(rawBytes);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(rawFile);
			CloseUtils.closeQuietly(newFileInput);
		}
	}
	
}
