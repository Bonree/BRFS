package com.bonree.brfs.common;

import java.util.UUID;

import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.proto.FileDataProtos.Fid.Builder;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年2月2日 上午11:26:44
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 
 *****************************************************************************
 */
public class Test {
    public static void main(String[] args) throws Exception {
        
        file();
    }
    
    public static void fid() throws Exception{
        Builder fid = Fid.newBuilder();
        fid.setVersion(1);
        fid.setCompress(2);
        fid.setReplica(2);
        fid.setStorageNameCode(123);
        fid.setTime(System.currentTimeMillis()/1000/60);
        fid.setOffset(12345);
        fid.setSize(2131);
        fid.setUuid(UUID.randomUUID().toString().replace("-", ""));
        fid.setServerId("1_2_45");
//        String buildStr = FidEncoder.build(fid);
//        Fids ffff = FidDecoder.build(buildStr);
        System.out.println("=====");
    }
    
    public static void file() throws Exception{
        
        String description = "2wrwerwerw11";
        String contents = "本文使用System提供的一个静态方法arraycopy()，实现数组间的复制。jquery+css五屏焦点图淡入淡出+圆形按钮切换广告图片代码."
                + "本文使用System提供的一个静态方法arraycopy()，实现数组间的复制。jquery+css五屏焦点图淡入淡出+圆形按钮切换广告图片代码."
                + "本文使用System提供的一个静态方法arraycopy()，实现数组间的复制。jquery+css五屏焦点图淡入淡出+圆形按钮切换广告图片代码."
                + "本文使用System提供的一个静态方法arraycopy()，实现数组间的复制。jquery+css五屏焦点图淡入淡出+圆形按钮切换广告图片代码.";
        
//        byte[] header = FileEncoder.header(5, 1);
//        byte[] file = FileEncoder.contents(contents, description, 3);
//        System.out.println(FileDecoder.version(header));
//        System.out.println(FileDecoder.validate(header));
//        System.out.println(FileDecoder.description(file));
//        System.out.println(FileDecoder.content(file));
    }
    
}
