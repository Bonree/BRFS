package com.bonree.brfs.block.codec.v1;

import com.bonree.brfs.block.codec.ByteHolder;

public class BlockEntry implements ByteHolder {
    private boolean compress;
    private String description;
    private byte[] content;
    private long crc;

    private static final int MASK_BIT = 7;
    private static final byte MASK = (byte) ((1 << MASK_BIT) - 1);

    @Override
    public byte[] toBytes() {
        return null;
    }

    private static byte[] encodeInt(int n) {
        byte[] bs = new byte[byteCount(n)];

        for (int i = bs.length - 1; i >= 0; i--) {
            bs[i] = (byte) (n & MASK);
            n = n >>> MASK_BIT;
        }

        bs[0] |= 1 << MASK_BIT;

        return bs;
    }

    private static int decodeInt(byte[] bs) {
        int n = 0;
        bs[0] &= MASK;

        for (int i = 0; i < bs.length; i++) {
            n |= bs[i];
            n = n << MASK_BIT;
        }

        return n;
    }

    private static int byteCount(int n) {
        int c = 1;
        while ((n = n >>> 7) != 0) {
            c++;
        }

        return c;
    }
}
