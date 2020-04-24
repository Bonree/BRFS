package com.bonree.brfs.disknode.data.write;

import com.bonree.brfs.common.utils.CloseUtils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 直接对文件进行数据写入的类，不使用缓存
 *
 * @author yupeng
 */
public class DirectFileWriter implements FileWriter {
    private RandomAccessFile file;
    private String filePath;
    private long fileLength;

    public DirectFileWriter(String filePath) throws IOException {
        this(new File(filePath));
    }

    public DirectFileWriter(File file) throws IOException {
        this(file, 0);
    }

    public DirectFileWriter(String filePath, long position) throws IOException {
        this(new File(filePath), position);
    }

    public DirectFileWriter(File file, long position) throws IOException {
        this.file = new RandomAccessFile(file, "rw");
        this.filePath = file.getAbsolutePath();
        position(position);
    }

    public DirectFileWriter(String filePath, boolean append) throws IOException {
        this(new File(filePath), append);
    }

    public DirectFileWriter(File file, boolean append) throws IOException {
        this.file = new RandomAccessFile(file, "rw");
        this.filePath = file.getAbsolutePath();
        position(append ? this.file.length() : 0);
    }

    @Override
    public String getPath() {
        return filePath;
    }

    @Override
    public void close() throws IOException {
        CloseUtils.closeQuietly(file);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(byte[] bytes, int offset, int size) throws IOException {
        try {
            file.write(bytes, offset, size);

            fileLength = file.length();
        } catch (IOException e) {
            //撤销错误写入数据
            file.setLength(fileLength);

            throw e;
        }
    }

    @Override
    public void flush() throws IOException {
        file.getFD().sync();
    }

    @Override
    public long position() {
        return fileLength;
    }

    @Override
    public void position(long pos) throws IOException {
        this.fileLength = pos < 0 ? 0 : pos;
        this.file.setLength(fileLength);
        this.file.seek(fileLength);
    }

}
