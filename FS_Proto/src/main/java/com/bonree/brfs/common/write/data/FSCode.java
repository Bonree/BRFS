package com.bonree.brfs.common.write.data;

import java.io.UnsupportedEncodingException;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年2月5日 下午5:04:13
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 编解码工具类
 * ****************************************************************************
 */
public class FSCode {
    private static final String hexStr = "0123456789ABCDEF";

    public static final byte[] start = {(byte) 0xAC};
    public static final byte[] tail = {(byte) 0xDA};

    /**
     * 概述：long数字转换成byte数组
     *
     * @param size   数字
     * @param length 转换后byte数组的长度
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] longToByte(long value, int length) {
        byte[] sizeByte = new byte[length];
        for (int i = 0; i < length; i++) {
            sizeByte[i] = (byte) ((value >> (i * 8)) & 0xFF);
        }
        return sizeByte;
    }

    public static byte[] stringToByte(String s) {
        try {
            return s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    /**
     * 概述：byte数组转换long数字
     *
     * @param bytes
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long byteToLong(byte[] bytes) {
        return byteToLong(bytes, 0, bytes.length);
    }

    /**
     * 概述：byte数组转换long数字
     *
     * @param bytes  源字节数组
     * @param pos    从源字节数组里取数据的开始位置
     * @param length 从源字节数组里读取字节的长度
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long byteToLong(byte[] bytes, int pos, int length) {
        long value = 0;
        int totalLength = pos + length;
        int offset = 0;
        for (int i = pos; i < totalLength; i++) {
            value |= (long) (bytes[i] & 0xFF) << (offset * 8);
            offset++;
        }
        return value;
    }

    public static String byteToString(byte[] bytes, int pos, int length) {
        try {
            return new String(bytes, pos, length, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 概述：byte转16进制
     *
     * @param bytes
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String byteToHex(byte[] bytes) {
        return byteToHex(bytes, 0, bytes.length);
    }

    /**
     * 概述：byte转16进制
     *
     * @param bytes  源字节数组
     * @param pos    从源字节数组里取数据的开始位置
     * @param length 从源字节数组里读取字节的长度
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String byteToHex(byte[] bytes, int pos, int length) {
        String hex = "";
        int totalLength = pos + length;
        for (int i = pos; i < totalLength; i++) {
            int high = (bytes[i] & 0xF0) >> 4;
            int low = bytes[i] & 0x0F;
            hex += String.valueOf(hexStr.charAt(high)); // 字节高4位
            hex += String.valueOf(hexStr.charAt(low)); // 字节低4位
        }
        return hex;
    }

    /**
     * 概述：16进制转byte
     *
     * @param hexString
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] hexToByte(String hexString) {
        int len = hexString.length() / 2; // hexString的长度对2取整，作为bytes的长度
        byte[] bytes = new byte[len];
        byte high = 0; // 字节高四位
        byte low = 0;  // 字节低四位
        hexString = hexString.toUpperCase();
        for (int i = 0; i < len; i++) {
            int highIndex = hexStr.indexOf(hexString.charAt(2 * i));
            int lowIndex = hexStr.indexOf(hexString.charAt(2 * i + 1));
            high = (byte) (highIndex << 4); // 右移四位得到高位
            low = (byte) lowIndex;
            bytes[i] = (byte) (high | low); // 高低位做或运算
        }
        return bytes;
    }

    /**
     * 概述：moreFlag编码
     *
     * @param value  要编码的数字
     * @param length moreflag后面可以表示其长度的位数(如编码后超过一个字节,那么从第二个字节开始moreflag就在高位上)
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] moreFlagEncoder(long value, int length) {
        int flag;
        int count = moreFlagLength(value, length); // 获取数字需要扩展的次数
        if (count == 0) { // 当前字节可以表示
            flag = 0 << length;
            return new byte[] {(byte) (flag | value)};
        }
        byte[] moreFlag = new byte[count + 1];
        flag = 1 << length;
        long temp = value >> (count * 7);
        moreFlag[0] = (byte) (flag | temp);
        for (int i = 1; i <= count; i++) {
            if (i == count) { // 表示最后一个byte,moreflag置为0
                flag = 0 << 7;
                moreFlag[i] = (byte) (flag | (value & 0x7F));
            } else {
                flag = 0xFF & (1 << 7); // 高位置1
                moreFlag[i] = (byte) (flag | value >> (count - i) * 7);
            }
        }
        return moreFlag;
    }

    /**
     * 概述：moreFlag解码
     *
     * @param bytes  要解码的数字字节数组
     * @param length moreflag后面可以表示其长度的位数(如编码后超过一个字节,那么从第二个字节开始moreflag就在高位上)
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long moreFlagDecoder(byte[] bytes, int length) {
        return moreFlagDecoder(bytes, length, 0);
    }

    /**
     * 概述：moreFlag解码
     *
     * @param bytes  要解码的数字字节数组
     * @param length moreflag后面可以表示其长度的位数(如编码后超过一个字节,那么从第二个字节开始moreflag就在高位上)
     * @param pos    字节数组开始的位置
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long moreFlagDecoder(byte[] bytes, int length, int pos) {
        long value = 0;
        int first = bytes[pos] & 0xFF; // 首字节处理
        int tail = (int) (Math.pow(2, length) - 1);
        int flag = (first >> length) & 0x01; // 首字节中的morefalg
        value |= (first & tail); // 首字节内容
        if (flag == 0) {
            return value;
        }
        for (int i = pos + 1; i < bytes.length; i++) {
            int temp = bytes[i] & 0xFF;
            value <<= 7;
            value |= (temp & 0x7F);
            if (temp >> 7 == 0) { // 高位为0退出
                break;
            }
        }
        return value;
    }

    /**
     * 概述：获取moreFlag扩展的次数
     *
     * @param value  moreFlag存储的数值
     * @param length moreflag后面可以表示其长度的位数
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int moreFlagLength(long value, int length) {
        int count = 0; //
        if (value < Math.pow(2, length)) { // 当前字节可以表示
            return count;
        }
        while (value >= Math.pow(2, (length + count * 7))) { // 判断数字需要扩展的次数
            count++;
        }
        return count;
    }

    /**
     * 概述：合并数组
     *
     * @param src 待合并的数组集合
     *
     * @return
     *
     * @throws Exception
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] addBytes(byte[]... src) throws Exception {
        int length = 0; // 获取每一byte数组的长
        int index = 0;  // 获取复制到目标数组的起始点，
        int totalLength = 0;
        for (int i = 0; i < src.length; i++) {
            if (src[i] != null) {
                totalLength += src[i].length;
            }
        }
        byte[] dest = new byte[totalLength]; // 目标数组
        for (int i = 0; i < src.length; i++) {
            if (src[i] != null) {
                length = src[i].length;
                System.arraycopy(src[i], 0, dest, index, length); // 将每一个byte[]复制到目标数组
                index += length; // 起始位置向后挪动byte[]的length
            }
        }
        return dest;
    }

    /**
     * 概述：截取指定长度的数组
     *
     * @param src   源字节数组
     * @param pos   截取的开始位置
     * @param count 截取的长度
     *
     * @return
     *
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] subBytes(byte[] src, int pos, int count) {
        byte[] dest = new byte[count];
        System.arraycopy(src, pos, dest, 0, count);
        return dest;
    }

}
