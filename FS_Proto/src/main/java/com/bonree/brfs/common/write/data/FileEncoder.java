package com.bonree.brfs.common.write.data;

import com.bonree.brfs.common.data.utils.GZipUtils;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;

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
     * @param validateType 校验标识 0:crc 1,2,3:保留
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] header(int version, int validateType) {
        int ver = version << 5;
        int val = validateType << 3;
        return new byte[] { (byte) (ver | val) };
    }

    /**
     * 概述：编码大文件的校验码
     * @param validate 校验码
     * @return
     * @throws Exception
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] validate(long validateCode) {
        return FSCode.LongToByte(validateCode, 8);
    }

    /**
     * 概述：编码一条消息
     * @param file 消息内容
     * @return
     * @throws Exception
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static byte[] contents(FileContent file) throws Exception {
        byte[] describeByte = null;
        byte[] describeLengthByte = null;
        int compressFlag = file.getCompress();
        String description = file.getDescription();
        byte[] content = file.getData().toByteArray();
        // 1.压缩
        int compress =  compressFlag << 6;

        byte[] validateByte = null;
        // 1.检验码
        int crcFlag = 0; // 标识校验码开关
        if (file.getCrcFlag()) {
            validateByte = FSCode.moreFlagEncoder(file.getCrcCheckCode(), 7);
            crcFlag = 1 << 5; // 标识校验码开关
        }
        // 2.描述
        if (description == null) {
            describeLengthByte = new byte[] { (byte) compress };
        } else {
            describeByte = description.getBytes("utf-8");
        }
        byte[] contentByte = null;
        byte[] contentLengthByte = null;
        // 3.内容
        if (content != null) {
            contentByte = content;
            contentLengthByte = FSCode.moreFlagEncoder(contentByte.length, 7);
        }
        if (compressFlag == 1) {        // gzip压缩
            describeByte = GZipUtils.compress(describeByte);
            contentByte = GZipUtils.compress(contentByte);
        } else if (compressFlag == 2) { // snappy压缩
            // describeByte =
            // contentByte =
        }

        describeLengthByte = FSCode.moreFlagEncoder(describeByte.length, 4);
        int describeLength = describeLengthByte[0] & 0xFF;
        describeLengthByte[0] = (byte) (compress | crcFlag | describeLength);
        
        if (contentByte != null) {
            contentLengthByte = FSCode.moreFlagEncoder(contentByte.length, 7);
        }

        return FSCode.addBytes(describeLengthByte, describeByte, contentLengthByte, contentByte, validateByte);
    }
}
