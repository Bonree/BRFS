package com.bonree.brfs.disknode.data.write;

import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.disknode.data.write.buf.FileBuffer;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用缓存的文件写入类，缓存满时会自动同步数据到磁盘。
 *
 * @author yupeng
 */
public class BufferedFileWriter implements FileWriter {
    private static final Logger LOG = LoggerFactory.getLogger(BufferedFileWriter.class);

    private RandomAccessFile file;
    private String filePath;
    private FileBuffer buffer;
    private long fileLength;
    private long position;

    public BufferedFileWriter(String filePath, FileBuffer buffer) throws IOException {
        this(new File(filePath), buffer);
    }

    public BufferedFileWriter(File file, FileBuffer buffer) throws IOException {
        this(file, 0, buffer);
    }

    public BufferedFileWriter(String filePath, long position, FileBuffer buffer) throws IOException {
        this(new File(filePath), position, buffer);
    }

    public BufferedFileWriter(File file, long position, FileBuffer buffer) throws IOException {
        this.file = new RandomAccessFile(file, "rw");
        this.filePath = file.getAbsolutePath();
        this.buffer = buffer;
        position(position);
    }

    public BufferedFileWriter(String filePath, boolean append, FileBuffer buffer) throws IOException {
        this(new File(filePath), append, buffer);
    }

    public BufferedFileWriter(File file, boolean append, FileBuffer buffer) throws IOException {
        this.file = new RandomAccessFile(file, "rw");
        this.filePath = file.getAbsolutePath();
        this.buffer = buffer;
        position(append ? this.file.length() : 0);
    }

    @Override
    public String getPath() {
        return filePath;
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        if (length > buffer.writableSize() && buffer.readableSize() > 0) {
            TimeWatcher tw = new TimeWatcher();
            flush();

            LOG.info("TIME_TEST flush for file[{}] take {} ms", filePath, tw.getElapsedTime());
        }

        if (length > buffer.capacity()) {
            //如果写入的数据超过了缓存大小，则直接写入文件，这种情况不需要对数据
            //进行缓存
            try {
                LOG.info("direct write data size[{}] to file[{}]", length, filePath);
                TimeWatcher tw = new TimeWatcher();
                file.getChannel().write(ByteBuffer.wrap(bytes, offset, length));
                LOG.info("TIME_TEST channel write for file[{}] take {} ms", filePath, tw.getElapsedTimeAndRefresh());

                fileLength = file.getChannel().position();
                LOG.info("TIME_TEST get position for file[{}] take {} ms", filePath, tw.getElapsedTime());

                LOG.debug("file length [{}] file[{}]", fileLength, filePath);
            } catch (IOException e) {
                file.getChannel().truncate(fileLength);

                throw e;
            } finally {
                position = fileLength;
            }

            return;
        }

        TimeWatcher tw = new TimeWatcher();
        buffer.write(bytes, offset, length);
        position += length;
        LOG.info("TIME_TEST buffer write for file[{}] take {} ms", filePath, tw.getElapsedTime());
        LOG.debug("write data size[{}] to buffer for file[{}], position become [{}]", length, filePath, position);
    }

    @Override
    public void flush() throws IOException {
        try {
            LOG.debug("flush data size[{}] to file[{}]", buffer.readableSize(), filePath);
            buffer.flush(file.getChannel());
            buffer.clear();

            fileLength = file.getChannel().position();
        } catch (IOException e) {
            file.getChannel().truncate(fileLength);

            throw e;
        } finally {
            position = fileLength;
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        CloseUtils.closeQuietly(file);
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public void position(long pos) throws IOException {
        buffer.clear();
        fileLength = pos < 0 ? 0 : pos;
        position = fileLength;
        file.setLength(fileLength);
        file.seek(fileLength);
    }
}
