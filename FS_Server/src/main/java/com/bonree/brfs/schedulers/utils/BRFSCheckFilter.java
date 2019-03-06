package com.bonree.brfs.schedulers.utils;

import com.bonree.brfs.common.files.impl.BRFSTimeFilter;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.write.data.FSCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.zip.CRC32;

public class BRFSCheckFilter extends BRFSTimeFilter{
    private static final Logger LOG = LoggerFactory.getLogger(BRFSCheckFilter.class);
    public BRFSCheckFilter(long startTime, long endTime){
        super(startTime,endTime);

    }

    @Override
    public boolean isAdd(String root, Map<String, String> values, boolean isFile){
        if(values.size() != keyMap.size()){
            return false;
        }
        if(!isFile){
            return false;
        }
        long time = BRFSPath.convertTime(values);
        if(startTime > 0 && time < startTime){
            return false;
        }
        if(endTime >0 && time >= endTime){
            return false;
        }
        String fileName = values.get(BRFSPath.FILE);
        if(fileName.contains(".")|| !fileName.contains("_")){
            return false;
        }
        if(fileName.contains(".rd")){
            return false;
        }
        String path = BRFSFileUtil.createPath(root,values);
        File rd = new File(path+".rd");
        if(rd.exists()){
            return false;
        }
        File file = new File(path);
        if(!file.exists()){
            return false;
        }
        return !check(file);
    }

    /**
     * 概述：校验文件crc
     * @param file
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public boolean check(File file) {
        RandomAccessFile raf = null;
        MappedByteBuffer buffer;
        String fileName = file.getName();
        try {
            if (!file.exists()) {
                LOG.warn("{}: not found!!", fileName);
                return false;
            }
            raf = new RandomAccessFile(file, "r");
            if(raf.length() <=0) {
                LOG.warn("{} : is empty",fileName);
                return false;
            }
            if (raf.readUnsignedByte() != 172) {
                LOG.warn("{}: Header byte is error!", fileName);
                return false;
            }
            if (raf.readUnsignedByte() != 0) {
                LOG.warn("{}: Header version is error!", fileName);
                return false;
            }
            CRC32 crc = new CRC32();
            raf.seek(0L);
            long size = raf.length() - 9L - 2L;

            if (size <= 0L) {
                LOG.warn("{}: No Content", fileName);
                return false;
            }
            buffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 2L, size);
            crc.update(buffer);
            raf.seek(raf.length() - 9L);
            byte[] crcBytes = new byte[8];
            int crcLen = raf.read(crcBytes);
            if(crcLen <=0){
                LOG.warn("{}: Tailer crc is empty!", fileName);
                return false;
            }
            LOG.debug("calc crc32 code :{}, save crc32 code :{}", crc.getValue(), FSCode.byteToLong(crcBytes));
            if (FSCode.byteToLong(crcBytes) != crc.getValue()) {
                LOG.warn("{}: Tailer CRC is error!", fileName);
                return false;
            }
            if (raf.readUnsignedByte() != 218) {
                LOG.warn("{}: Tailer byte is error!", fileName);
                return false;
            }
            return true;
        }
        catch (Exception e) {
            LOG.error("check error {}:{}",fileName,e);
        }finally {
            if (raf != null) {
                try {
                    raf.close();
                }
                catch (IOException e) {
                    LOG.error("close {}:{}",fileName,e);
                }
            }
        }
        return false;
    }
}
