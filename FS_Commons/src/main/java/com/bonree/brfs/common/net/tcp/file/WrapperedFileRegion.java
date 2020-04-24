package com.bonree.brfs.common.net.tcp.file;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class WrapperedFileRegion extends AbstractReferenceCounted implements FileRegion {
    private final long position;
    private final long count;
    private long transferred;
    private FileChannel file;

    /**
     * Create a new instance
     *
     * @param file     the {@link FileChannel} which should be transfered
     * @param position the position from which the transfer should start
     * @param count    the number of bytes to transfer
     */
    public WrapperedFileRegion(FileChannel file, long position, long count) {
        if (file == null) {
            throw new NullPointerException("file");
        }
        if (position < 0) {
            throw new IllegalArgumentException("position must be >= 0 but was " + position);
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0 but was " + count);
        }
        this.file = file;
        this.position = position;
        this.count = count;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public long count() {
        return count;
    }

    @Deprecated
    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long transferred() {
        return transferred;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        long count = this.count - position;
        if (count < 0 || position < 0) {
            throw new IllegalArgumentException(
                "position out of range: "
                    + position
                    + " (expected: 0 - "
                    + (this.count - 1) + ')');
        }
        if (count == 0) {
            return 0L;
        }
        if (refCnt() == 0) {
            throw new IllegalReferenceCountException(0);
        }

        long written = file.transferTo(this.position + position, count, target);
        if (written > 0) {
            transferred += written;
        }
        return written;
    }

    @Override
    protected void deallocate() {
    }

    @Override
    public FileRegion retain() {
        super.retain();
        return this;
    }

    @Override
    public FileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public FileRegion touch() {
        return this;
    }

    @Override
    public FileRegion touch(Object hint) {
        return this;
    }
}

