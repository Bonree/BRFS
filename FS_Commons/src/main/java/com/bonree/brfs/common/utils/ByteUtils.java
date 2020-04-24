package com.bonree.brfs.common.utils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public final class ByteUtils {
    ;

    public static long crc(byte[] bytes) {
        return crc(bytes, 0, bytes.length);
    }

    public static long crc(byte[] bytes, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(bytes, offset, length);

        return crc32.getValue();
    }

    public static long crc(ByteBuffer buf) {
        CRC32 crc32 = new CRC32();
        crc32.update(buf);

        return crc32.getValue();
    }
}
