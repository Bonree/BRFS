package com.bonree.brfs.disknode.data.read;

import com.bonree.brfs.common.utils.BufferUtils;
import com.bonree.brfs.common.utils.CloseUtils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class DataFileReader {

    private DataFileReader() {
    }

    public static byte[] readFile(File file, long offset, int size) {
        return readFile(file.getAbsolutePath(), offset, size);
    }

    public static byte[] readFile(File file) {
        return readFile(file, 0, Integer.MAX_VALUE);
    }

    public static byte[] readFile(File file, long offset) {
        return readFile(file, offset, Integer.MAX_VALUE);
    }

    public static byte[] readFile(String filePath, long offset) {
        return readFile(filePath, offset, Integer.MAX_VALUE);
    }

    public static byte[] readFile(String filePath, long offset, int size) {
        RandomAccessFile file = null;
        MappedByteBuffer buffer = null;
        try {
            file = new RandomAccessFile(filePath, "r");
            long fileLength = file.length();

            if (offset >= fileLength) {
                return new byte[0];
            }

            offset = Math.max(0, offset);
            size = (int) Math.min(size, file.length() - offset);
            byte[] bytes = new byte[size];
            buffer = file.getChannel().map(MapMode.READ_ONLY, offset, size);
            buffer.get(bytes);

            return bytes;
        } catch (IOException e) {
            // ignore
        } finally {
            CloseUtils.closeQuietly(file);
            BufferUtils.release(buffer);
        }

        return new byte[0];
    }
}
