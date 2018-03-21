package com.br.disknode.record;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import com.br.disknode.utils.ProtoStuffUtils;

public class RecordReader {
	private InputStream recordInput;
	
	private RecordReader(InputStream input) {
		this.recordInput = input;
	}
	
	public static RecordReader get(String filePath) {
		File file = new File(filePath);
		RecordReader reader = null;
		try {
			InputStream input = new FileInputStream(file);
			reader = new RecordReader(input);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return reader;
	}
	
	public RecordElement next() {
		return ProtoStuffUtils.readFrom(recordInput, RecordElement.class);
	}
}
