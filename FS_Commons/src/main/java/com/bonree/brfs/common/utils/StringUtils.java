package com.bonree.brfs.common.utils;

import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

public class StringUtils {

    private static final char SEPARATOR_DIR = '/';

    /** 概述：对basePath进行修剪
     * @param basePath 如：/aaa/bbb/ or /aaa/bbb
     * @return 修剪后的路径 如：/aaa/bbb
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public static String trimBasePath(String basePath) {
        String newBasePath = null;
        if (!org.apache.commons.lang3.StringUtils.isEmpty(basePath)) {
            byte ch = basePath.getBytes()[basePath.length() - 1];
            if (ch == SEPARATOR_DIR) {
                newBasePath = basePath.substring(0, basePath.length() - 1);
            } else {
                newBasePath = basePath;
            }
        }
        return newBasePath;
    }

    /** 概述：对basePath进行规范
     * @param basePath 如：/aaa/bbb/ or /aaa/bbb
     * @return 规范后的路径 如：/aaa/bbb/
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public static String normalBasePath(String basePath) {
        String newBasePath = null;
        if (!org.apache.commons.lang3.StringUtils.isEmpty(basePath)) {
            byte ch = basePath.getBytes()[basePath.length() - 1];
            if (ch == SEPARATOR_DIR) {
                newBasePath = basePath;
            } else {
                newBasePath = basePath + SEPARATOR_DIR;
            }
        }
        return newBasePath;
    }

    /**
     * 安静的把字符串转化为UTF-8格式的字节数组
     * 
     * @param s
     * @return
     */
    public static byte[] toUtf8Bytes(String s) {
		try {
			return s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		return new byte[0];
	}
    /**
     * 判断字符串是否为空
     *
     * @param args 字符串
     * @return 空 true,不为空 false
     * @author 张念礼
     * @date 2012-4-13 上午10:05:55
     * @version V1.0.0
     * @ModifyRecord 修改记录
     * <br>1、张念礼 ; 2012-4-13 上午10:05:55; 初始化
     */
    public static final Boolean isEmpty(String args) {
        Boolean bool = false;
        //空对象
        if (null == args) {
            bool = true;
        }
        //空字符串
        else if ("".equals(args.trim())) {
            bool = true;
        }
        //全角
        else if ("　".equals(args)) {
            bool = true;
        }
        //null字符串
        else if ("null".equals(args)) {
            bool = true;
        } else if ("[]".equals(args)) {
            bool = true;
        }
        return bool;
    }
    
    /**
     * 概述：分割字符串
     *
     * @param str   字段串
     * @param delim 分割符号
     * @return String[]
     * @Title: getSplit
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static final String[] getSplit(String str, String delim) {
        if (str == null || delim == null) {
            return null;
        }
        StringTokenizer token = new StringTokenizer(str, delim);
        int num = token.countTokens();
        String[] result = new String[num];
        int i = 0;
        while (token.hasMoreTokens()) {
            result[i++] = token.nextToken();
        }
        return result;
    }
}
