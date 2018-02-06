package com.bonree.brfs.server.utils;

import com.bonree.brfs.common.code.FSCode;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年2月3日 下午6:43:11
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 文件编码
 *****************************************************************************
 */
public class FileEncoder {

    /**
     * 概述：消息开头
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] start() {
        return FSCode.start;
    }

    /**
     * 概述：消息结尾
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] tail() {
        return FSCode.tail;
    }

    /**
     * 概述：编码文件的header
     * @param version 协议版本
     * @param validateFlag 校验标识 0:不开启 1.crc 2和3:保留
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] header(int version, int validateFlag) {
        int ver = version << 5;
        int val = validateFlag << 3;
        return new byte[] { (byte) (ver | val) };
    }

    /**
     * 概述：编码校验码
     * @param validate 校验码
     * @return
     * @throws Exception
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public byte[] validate(long validateCode) throws Exception {
        return FSCode.LongToByte(validateCode, 8);
    }

    /**
     * 概述：编码一条消息
     * @param contents 消息内容
     * @param description 描述信息
     * @param compress 是否压缩
     * @return
     * @throws Exception
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] contents(String content, String description, int compress) throws Exception {
        byte[] describeByte = null;
        byte[] describeLengthByte = null;
        // 压缩
        int c = compress << 6;
        // 描述
        if (description == null) {
            describeLengthByte = new byte[] { (byte) c };
        } else {
            describeByte = description.getBytes("utf-8");
            describeLengthByte = FSCode.moreFlagEncoder(describeByte.length, 5);
            describeLengthByte[0] = (byte) (c | (describeLengthByte[0] & 0xFF));
        }
        byte[] contentByte = null;
        byte[] contentLengthByte = null;
        // 内容
        if (content != null) {
            contentByte = content.getBytes("utf-8");
            contentLengthByte = FSCode.moreFlagEncoder(contentByte.length, 7);
        }
        if (compress == 1) {        // gzip压缩
            // describeByte =
            // contentByte =
        } else if (compress == 2) { // snappy压缩
            // describeByte =
            // contentByte =
        }
        return FSCode.addBytes(describeLengthByte, describeByte, contentLengthByte, contentByte);
    }

    /**
     * 概述：编码一条消息
     * @param contents 消息内容
     * @return
     * @throws Exception
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] contents(String content) throws Exception {
        return contents(content, null, 0);
    }
}
