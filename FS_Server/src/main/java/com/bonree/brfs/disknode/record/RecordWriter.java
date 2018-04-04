package com.bonree.brfs.disknode.record;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.bonree.brfs.common.utils.ProtoStuffUtils;

public class RecordWriter {
	public static final String RECORD_FILE_EXTEND = ".rd";
	
	private OutputStream recordOutput;

	private RecordWriter(OutputStream output) {
		this.recordOutput = output;
	}

	public static RecordWriter get(String filePath) {
		return get(new File(filePath));
	}

	public static RecordWriter get(File file) {
		RecordWriter writer = null;
		try {
			OutputStream output = new FileOutputStream(file);
			writer = new RecordWriter(output);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return writer;
	}

	public void record(RecordElement record) throws IOException {
		ProtoStuffUtils.writeTo(recordOutput, record);
		recordOutput.flush();
	}
	
	public void close() {
		try {
			recordOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void destory() {
		
	}
}
