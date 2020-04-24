package com.bonree.brfs.disknode.utils;

import com.bonree.brfs.common.utils.CloseUtils;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.CRC32;

public final class CheckUtils {

    public static long check(String path) {
        CRC32 crc32 = new CRC32();

        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(path, "r");
            //跳过两字节的头部
            file.skipBytes(2);

            byte[] buf = new byte[8096];
            int read = -1;
            while ((read = file.read(buf)) != -1) {
                crc32.update(buf, 0, read);
            }

            return crc32.getValue();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            CloseUtils.closeQuietly(file);
        }

        return 0;
    }

    public static long checkCompleted(String path) {
        CRC32 crc32 = new CRC32();

        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(path, "r");
            //跳过两字节的头部
            file.skipBytes(2);
            int length = (int) (file.length() - 9);

            byte[] buf = new byte[8096];
            int read = -1;
            while ((read = file.read(buf, 0, length)) != -1) {
                crc32.update(buf, 0, read);
                length -= read;
            }

            return crc32.getValue();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            CloseUtils.closeQuietly(file);
        }

        return 0;
    }
}
