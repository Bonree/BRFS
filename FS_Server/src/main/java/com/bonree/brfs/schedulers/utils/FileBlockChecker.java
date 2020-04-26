package com.bonree.brfs.schedulers.utils;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.write.data.FSCode;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年6月20日 上午9:42:57
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 文件收集
 *****************************************************************************
 */
public class FileBlockChecker {
    private static final Logger LOG = LoggerFactory.getLogger(FileBlockChecker.class);

    /**
     * 概述：检查单个文件
     *
     * @param path
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static boolean check(String path) {
        if (BrStringUtils.isEmpty(path)) {
            return false;
        }
        File file = new File(path);
        return check(file);
    }

    /**
     * 概述：校验文件crc
     *
     * @param file
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static boolean check(File file) {
        RandomAccessFile raf = null;
        MappedByteBuffer buffer;
        String fileName = file.getName();
        try {
            if (!file.exists()) {
                LOG.warn("{}: not found!!", fileName);
                return false;
            }
            raf = new RandomAccessFile(file, "r");
            if (raf.length() <= 0) {
                LOG.warn("{} : is empty", fileName);
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
            if (crcLen <= 0) {
                LOG.warn("{}: Tailer CRC is empty !!", fileName);
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
        } catch (Exception e) {
            LOG.error("check error {}:{}", fileName, e);
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    LOG.error("close {}:{}", fileName, e);
                }
            }
        }
        return false;
    }

}
