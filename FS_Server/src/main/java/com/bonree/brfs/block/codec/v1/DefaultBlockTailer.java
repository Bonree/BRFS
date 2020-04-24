package com.bonree.brfs.block.codec.v1;

import com.bonree.brfs.block.codec.ByteHolder;
import com.bonree.brfs.block.codec.ByteHolderBuilder;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class DefaultBlockTailer implements ByteHolder {
    public static final byte TAIL_BYTE = (byte) 0xDA;
    private static final int TAIL_LENGTH = Long.BYTES + Byte.BYTES;
    private final long crcCode;

    public DefaultBlockTailer(long crcCode) {
        this.crcCode = crcCode;
    }

    public long crc() {
        return crcCode;
    }

    public int length() {
        return TAIL_LENGTH;
    }

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(TAIL_LENGTH).putLong(crcCode).put(TAIL_BYTE).array();
    }

    public static class Builder implements ByteHolderBuilder<DefaultBlockTailer> {
        private CRC32 crc = new CRC32();

        public void update(byte[] bytes) {
            crc.update(bytes);
        }

        public DefaultBlockTailer build() {
            return new DefaultBlockTailer(crc.getValue());
        }
    }
}
