package com.bonree.brfs.common.write.data;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.common.data.utils.Base64;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年1月29日 下午2:05:46
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: fid解码
 *****************************************************************************
 */
public class FidDecoder {

    /**
     * 概述：fid信息解码
     * @param fidStr base64格式的fid
     * @return
     * @throws Exception
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static Fid build(String fidStr) throws Exception {
        Fid.Builder fid = Fid.newBuilder();
        if (fidStr != null) {
            ByteArrayInputStream input  = new ByteArrayInputStream(Base64.decode(fidStr, Base64.DEFAULT));
            System.out.println("read : " + input.available());
            byte[] bytes = new byte[16];
            input.read(bytes, 0, 1);
            //跳过开头
            
            input.read(bytes, 0, 1);
            fid.setVersion(version(bytes));
            fid.setCompress(compress(bytes));
            
            input.read(bytes, 0, 2);
            fid.setStorageNameCode(storageName(bytes));
            
            input.read(bytes, 0, 16);
            fid.setUuid(uuid(bytes));
            
            input.read(bytes, 0, 5);
            fid.setTime(time(bytes));
            
            input.read(bytes, 0, 4);
            fid.setOffset(offset(bytes));
            
            input.read(bytes, 0, 4);
            fid.setSize(size(bytes));
            
            fid.addAllServerId(serverId(input));
            fid.setDuration(duration(input));
        }
        return fid.build();
    }

    /**
     * 概述：解码version
     * @param header
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int version(byte[] bytes) {
        int h = bytes[0] & 0xff;
        return h >> 5;
    }

    /**
     * 概述：解码compress
     * @param header
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int compress(byte[] bytes) {
        int h = bytes[0] & 0xff;
        return h >> 3 & 0x03;
    }

    /**
     * 概述：解码storageName
     * @param bytes
     * @return
     * @throws Exception 
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long storageName(byte[] bytes) throws Exception {
        return FSCode.byteToLong(bytes, 0, 2);
    }

    /**
     * 概述：解码uuid
     * @param bytes
     * @return
     * @throws Exception 
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String uuid(byte[] bytes) throws Exception {
        return FSCode.ByteToHex(bytes, 0, 16);
    }

    /**
     * 概述：解码time
     * @param timeByte
     * @return
     * @throws Exception 
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long time(byte[] bytes) throws Exception {
        return FSCode.byteToLong(bytes, 0, 5) * 60 * 1000;
    }
    
    public static String duration(InputStream input) throws IOException {
    	byte[] bytes = new byte[input.available()];
    	input.read(bytes);
    	return FSCode.byteToString(bytes, 0, bytes.length);
    }

    /**
     * 概述：解码offset
     * @param offsetByte
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long offset(byte[] offsetByte) {
        return FSCode.byteToLong(offsetByte, 0, 4);
    }

    /**
     * 概述：解码size
     * @param sizeByte
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long size(byte[] sizeByte) {
        return FSCode.byteToLong(sizeByte, 0, 4);
    }

    /**
     * 概述：解码serverId
     * @param serverIdByte
     * @return
     * @throws IOException 
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static List<Integer> serverId(InputStream input) throws IOException {
        int temp = 0;
        List<Integer> sidList = new ArrayList<Integer>();
        byte[] b = new byte[1];
        while (input.read(b) != -1) {
        	if(b[0] == 0) {
        		break;
        	}
        	
            int sid = b[0] & 0xFF;
            if (sid >> 7 == 0) {
                temp <<= 7;
                sidList.add(temp | (sid & 0x7F));
                temp = 0;
            } else {
                temp = sid & 0x7F;
            }
        }
        return sidList;
    }

}
