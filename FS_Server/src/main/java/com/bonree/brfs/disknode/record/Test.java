package com.br.disknode.record;

import java.io.IOException;
import java.util.zip.CRC32;

public class Test {

	public static void main(String[] args) throws IOException, ClassNotFoundException {
//		RecordWriter writer = RecordWriter.get("/root/temp/log.rd");
//		
//		RecordElement[] records = new RecordElement[5];
//		
//		CRC32 crc = new CRC32();
//		crc.update("12345".getBytes());
//		records[0] = new RecordElement(0, 100, crc.getValue());
//		records[1] = new RecordElement(100, 145);
//		records[2] = new RecordElement(145, 234);
//		records[3] = new RecordElement(234, 300, crc.getValue());
//		records[4] = new RecordElement(300, 342, crc.getValue());
//		
//		for(RecordElement record : records) {
//			writer.record(record);
//		}
		
		RecordReader reader = RecordReader.get("/root/temp/t1.rd");
		
		RecordElement record = null;
		while((record = reader.next()) != null) {
			System.out.println("--" + record);
		}
	}

}
