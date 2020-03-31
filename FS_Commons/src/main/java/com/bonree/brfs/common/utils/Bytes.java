package com.bonree.brfs.common.utils;

/**
 * @author wangchao
 * @date 2020/3/19 - 11:38 上午
 */
public class Bytes {

    /**
     * Size of int in bytes
     */
    public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

    //b是否在a的开头，ab长度不检查，使用者自行检查
    public static boolean startWith(byte[] a , byte[] b){
        for (int i = 0 ; i < b.length ; i++){
            if(a[i]!=b[i]) return false;
        }
        return true;
    }

    public static byte[] int2Bytes(int num) {
        byte[] byteNum = new byte[4];
        for (int ix = 0; ix < 4; ++ix) {
            int offset = 32 - (ix + 1) * 8;
            byteNum[ix] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }

    public static int bytes2Int(byte[] byteNum) {
        int num = 0;
        for (int ix = 0; ix < 4; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }

    public static byte int2OneByte(int num) {
        return (byte) (num & 0x000000ff);
    }

    public static int oneByte2Int(byte byteNum) {
        return byteNum > 0 ? byteNum : (128 + (128 + byteNum));
    }

    public static byte[] long2Bytes(long num) {
        byte[] byteNum = new byte[8];
        for (int ix = 0; ix < 8; ++ix) {
            int offset = 64 - (ix + 1) * 8;
            byteNum[ix] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }

    public static long bytes2Long(byte[] byteNum) {
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }

    public static void main(String[] args) {
        int num = 129;
        System.out.println("测试的int值为:" + num);

        byte[] int2bytes = Bytes.int2Bytes(num);
        System.out.printf("int转成bytes: ");
        for (int i = 0; i < 4; ++i) {
            System.out.print(int2bytes[i] + " ");
        }
        System.out.println();

        int bytes2int = Bytes.bytes2Int(int2bytes);
        System.out.println("bytes转行成int: " + bytes2int);

        byte int2OneByte = Bytes.int2OneByte(num);
        System.out.println("int转行成one byte: " + int2OneByte);

        int oneByte2Int = Bytes.oneByte2Int(int2OneByte);
        System.out.println("one byte转行成int: " + oneByte2Int);
        System.out.println();

        long longNum = 100000;
        System.out.println("测试的long值为：" + longNum);

        byte[] long2Bytes = Bytes.long2Bytes(longNum);
        System.out.printf("long转行成bytes: ");
        for (int ix = 0; ix < long2Bytes.length; ++ix) {
            System.out.print(long2Bytes[ix] + " ");
        }
        System.out.println();

        long bytes2Long = Bytes.bytes2Long(long2Bytes);
        System.out.println("bytes转行成long: " + bytes2Long);
    }

}
