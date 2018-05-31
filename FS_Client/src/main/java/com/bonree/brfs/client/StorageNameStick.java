package com.bonree.brfs.client;

import java.io.Closeable;
import java.text.ParseException;
import java.util.Date;

public interface StorageNameStick extends Closeable {
	String[] writeData(InputItem[] itemArrays);
	String writeData(InputItem item);
	InputItem readData(String fid) throws Exception;
	boolean deleteData(String startTime, String endTime);
	boolean deleteData(String startTime, String endTime,String dateForamt) throws ParseException;
	boolean deleteData(long startTime, long endTime);
	boolean deleteData(Date startTime, Date endTime);
}
