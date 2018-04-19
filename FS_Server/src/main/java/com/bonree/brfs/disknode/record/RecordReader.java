package com.bonree.brfs.disknode.record;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;

public class RecordReader implements Closeable {
	private InputStream recordInput;
	
	private RecordReader(InputStream input) {
		this.recordInput = input;
	}
	
	public static RecordReader from(String filePath) {
		File file = new File(filePath);
		RecordReader reader = null;
		try {
			BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
			reader = new RecordReader(input);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return reader;
	}
	
	public RecordElement next() {
		return ProtoStuffUtils.readFrom(recordInput, RecordElement.class);
	}

	@Override
	public void close() throws IOException {
		CloseUtils.closeQuietly(recordInput);
	}
}
