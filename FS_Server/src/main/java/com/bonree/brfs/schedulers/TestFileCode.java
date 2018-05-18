package com.bonree.brfs.schedulers;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.CRC32;

import com.bonree.brfs.common.proto.FileDataProtos.FileContent;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.write.data.FileDecoder;
import com.bonree.brfs.common.write.data.FileEncoder;
import com.google.protobuf.ByteString;

public class TestFileCode {
	public static void main(String[] args) {
		try {
			file();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
public static void file() throws Exception{
        
        int compress = 1;
        String description = "2wrwerwerw11";
        String contents = "本文使用System提供的一个静态方法arraycopy()，实现数组间的复制。jquery+css五屏焦点图淡入淡出+圆形按钮切换广告图片代码."
                + "本文使用System提供的一个静态方法arraycopy()，实现数组间的复制。jquery+css五屏焦点图淡入淡出+圆形按钮切换广告图片代码."
                + "本文使用System提供的一个静态方法arraycopy()，实现数组间的复制。jquery+css五屏焦点图淡入淡出+圆形按钮切换广告图片代码."
                + "本文使用System提供的一个静态方法arraycopy()，实现数组间的复制。jquery+css五屏焦点图淡入淡出+圆形按钮切换广告图片代码.";
        boolean crcFlag = true;
        long crcCode = 1232545253;
        FileContent.Builder file = FileContent.newBuilder();
        file.setCompress(compress);
        file.setDescription(description);
        byte[] data = contents.getBytes();
        file.setData(ByteString.copyFrom(data));
        file.setCrcFlag(crcFlag);
        file.setCrcCheckCode(crcCode);
        
        byte[] header = FileEncoder.header(5, 1);
        System.out.println(FileDecoder.version(header));
        System.out.println(FileDecoder.validate(header));
        
        byte[] fileByte = FileEncoder.contents(file.build());
        System.out.println(FileDecoder.contents(fileByte));
       
    }
public static boolean checkCompleted(String path) {
	CRC32 crc32 = new CRC32();
	
	RandomAccessFile file = null;
	try {
		file = new RandomAccessFile(path, "r");
		//跳过2字节的头部
		file.skipBytes(2);
	
		int length = (int) (file.length() - 9);
		
		byte[] buf = new byte[8096];
		int read = -1;
		while((read = file.read(buf, 0, length)) != -1) {
			crc32.update(buf, 0, read);
			length -= read;
		}
		
		byte[] crcKeys = new byte[8];
		file.read(crcKeys, 0, 8);
		long crcNum = FileDecoder.validate(crcKeys);
		if(crcNum == crc32.getValue()){
			return true;
		}
	} catch (IOException e) {
		e.printStackTrace();
	} finally {
		CloseUtils.closeQuietly(file);
	}
	return false;
}
}
