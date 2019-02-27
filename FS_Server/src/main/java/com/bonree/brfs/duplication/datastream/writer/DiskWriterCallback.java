package com.bonree.brfs.duplication.datastream.writer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.duplication.FidBuilder;
import com.bonree.brfs.duplication.datastream.dataengine.impl.DataObject;
import com.bonree.brfs.duplication.datastream.file.FileObject;
import com.bonree.brfs.duplication.datastream.writer.DiskWriter.WriteProgressListener;

public class DiskWriterCallback {
	private static final Logger LOG = LoggerFactory.getLogger(DiskWriterCallback.class);
	
	private AtomicInteger count;
	private AtomicReferenceArray<DataOut[]> results;
	
	private List<DataObject> dataCallbacks;
	
	private WriteProgressListener callback;
	
	public DiskWriterCallback(int dupCount, List<DataObject> dataCallbacks, WriteProgressListener callback) {
		this.count = new AtomicInteger(dupCount);
		this.results = new AtomicReferenceArray<DataOut[]>(dupCount);
		this.dataCallbacks = dataCallbacks;
		this.callback = callback;
	}

	public void complete(FileObject file, int index, DataOut[] result) {
		results.set(index, result);
		
		if(count.decrementAndGet() == 0) {
			handleResults(file);
		}
	}
	
	private void handleResults(FileObject file) {
		int maxValidIndex = -1;
		DataOut[] maxResult = null;
		
		boolean writeError = false;
		for(int i = 0; i < results.length(); i++) {
			DataOut[] dupResult = results.get(i);
			for(int j = dupResult.length - 1; j >= 0; j--) {
				if(dupResult[j] == null) {
					writeError = true;
					continue;
				}
				
				if(j != dupResult.length - 1) {
					LOG.error("data write error from index[{}] to index[{}] in file[{}]", j + 1, dupResult.length - 1, file.node().getName());
				}
				
				if(maxValidIndex < j) {
					maxValidIndex = j;
					maxResult = dupResult;
				}
				
				break;
			}
		}
		
		LOG.info("write result with max valid index[{}] in file[{}]", maxValidIndex, file.node().getName());
		file.setLength(maxValidIndex < 0 ? file.length() : (maxResult[maxValidIndex].offset() + maxResult[maxValidIndex].length()));
		callback.writeCompleted(file, writeError);
		
		String[] fids = new String[dataCallbacks.size()];
		for(int i = 0; i <= maxValidIndex; i++) {
			long offset = maxResult[i].offset();
			int size = maxResult[i].length();
			
			fids[i] = FidBuilder.getFid(file.node(), offset, size);
		}
		
		for(int i = 0; i < fids.length; i++) {
			dataCallbacks.get(i).processComplete(fids[i]);
		}
	}
}
