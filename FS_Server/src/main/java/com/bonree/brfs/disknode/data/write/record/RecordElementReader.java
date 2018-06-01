package com.bonree.brfs.disknode.data.write.record;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;

public class RecordElementReader implements Iterable<RecordElement>, Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(RecordElementReader.class);
	
	private File recordFile;
	private InputStream fileInput;
	
	public RecordElementReader(File recordFile) {
		this.recordFile = recordFile;
	}

	@Override
	public Iterator<RecordElement> iterator() {
		try {
			fileInput = new BufferedInputStream(new FileInputStream(recordFile));
		} catch (FileNotFoundException e) {
			LOG.error("open RecordElementIterator[{}] error..", recordFile.getAbsolutePath(), e);
		}
		
		return new RecordElementIterator(fileInput);
	}

	@Override
	public void close() throws IOException {
		CloseUtils.closeQuietly(fileInput);
	}
	
	private class RecordElementIterator implements Iterator<RecordElement> {
		private InputStream recordInput;
		private RecordElement next;
		
		public RecordElementIterator(InputStream recordInput) {
			this.recordInput = recordInput;
			readNext();
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public RecordElement next() {
			RecordElement result = next;
			readNext();
			
			return result;
		}
		
		private void readNext() {
			next = ProtoStuffUtils.readFrom(recordInput, RecordElement.class);
		}
	}
}
