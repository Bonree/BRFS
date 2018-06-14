package com.bonree.brfs.disknode.server.handler;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.write.data.FileDecoder;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordElement;
import com.bonree.brfs.disknode.data.write.record.RecordElementReader;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class SequenceNumberCache {
	private static final Logger LOG = LoggerFactory.getLogger(SequenceNumberCache.class);
	private FileWriterManager writerManager;
	
	private LoadingCache<String, Optional<Map<Integer, RecordElement>>> recordCache = CacheBuilder.newBuilder()
			.maximumSize(10)
			.expireAfterAccess(10, TimeUnit.SECONDS)
			.build(new SequenceLoader());
	
	public SequenceNumberCache(FileWriterManager writerManager) {
		this.writerManager = writerManager;
	}
	
	private Map<Integer, RecordElement> getInner(String filePath) {
		try {
			Optional<Map<Integer, RecordElement>> optional = recordCache.get(filePath);
			if(!optional.isPresent()) {
				recordCache.invalidate(filePath);
				return null;
			}
			
			return optional.orNull();
		} catch (ExecutionException e) {
			LOG.error("get element from cache by key[{}] error", filePath);
		}
		
		return null;
	}
	
	public Map<Integer, RecordElement> get(String filePath) {
		return get(filePath, false);
	}
	
	public Map<Integer, RecordElement> get(String filePath, boolean refresh) {
		if(refresh) {
			recordCache.invalidate(filePath);
		}
		
		return getInner(filePath);
	}
	
	private class SequenceLoader extends CacheLoader<String, Optional<Map<Integer, RecordElement>>> {

		@Override
		public Optional<Map<Integer, RecordElement>> load(String filePath) throws Exception {
			HashMap<Integer, RecordElement> recordInfo = null;
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(filePath, false);
			if(binding != null) {
				RecordElementReader recordReader = null;
				try {
					binding.first().flush();
					writerManager.adjustFileWriter(filePath);
					
					RecordCollection recordSet = binding.first().getRecordCollection();
					
					recordReader = recordSet.getRecordElementReader();
					
					recordInfo = new HashMap<Integer, RecordElement>();
					for(RecordElement element : recordReader) {
						recordInfo.put(element.getSequence(), element);
					}
				} catch (Exception e) {
					LOG.error("getSequnceNumbers from file[{}] error", filePath, e);
				} finally {
					CloseUtils.closeQuietly(recordReader);
				}
			} else {
				//到这有两种情况：
				//1、文件打开操作未成功后进行同步；
				//2、文件关闭操作未成功进行再次关闭;
				recordInfo = new HashMap<Integer, RecordElement>();
				File dataFile = new File(filePath);
				if(dataFile.exists()) {
					//到这的唯一机会是，多副本文件关闭时只有部分关闭成功，当磁盘节点恢复正常
					//后，需要再次进行同步流程让所有副本文件关闭，因为没有日志文件，所以只能
					//通过解析数据文件生成序列号列表
					byte[] bytes = DataFileReader.readFile(dataFile, 0);
					if(bytes[0] == 0xAC && bytes[1] == 0) {
						recordInfo.put(0, new RecordElement(0, 0, 2, 0));
						
						List<String> offsetInfos = FileDecoder.getOffsets(bytes);
						int index = 1;
						for(String info : offsetInfos) {
							List<String> parts = Splitter.on('|').splitToList(info);
							int offset = Integer.parseInt(parts.get(0));
							int size = Integer.parseInt(parts.get(1));
							recordInfo.put(index, new RecordElement(index, offset, size, 0));
							index++;
						}
					}
				}
			}
			
			return Optional.fromNullable(recordInfo);
		}
		
	}
}
