package com.bonree.brfs.resouceschedule.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;


/**
 * 字符串处理工具类
 *
 * @author 张念礼
 * @version V1.0.0
 * @date 2012-4-13 上午09:56:51
 * @Copyright Copyright © 2011 bonree. All rights reserved.
 */
public class StringUtils {

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
     * 判断字符串是否不为空
     *
     * @param args 字符串
     * @return 空 false,不为空  true
     * @author 张念礼
     * @date 2012-4-13 上午10:14:29
     * @version V1.0.0
     * @ModifyRecord 修改记录
     * <br>1、张念礼 ; 2012-4-13 上午10:14:29; 初始化
     */
    public static final Boolean isNotEmpty(String args) {
        return !isEmpty(args);
    }

    /**
     * 验证数组数据是否为空
     *
     * @param args 字符串数组
     * @return 空 true,不为空 false
     * @author 张念礼
     * @date 2012-4-13 上午10:43:48
     * @version V1.0.0
     * @ModifyRecord 修改记录
     * <br>1、张念礼 ; 2012-4-13 上午10:43:48; 初始化
     */
    public static final Boolean arrayIsEmpty(String... args) {
        if (null == args) {
            return true;
        }
        for (int i = 0; i < args.length; i++) {
            if (StringUtils.isEmpty(args[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * 验证数组数据是否不为空
     *
     * @param args 字符串数组
     * @return 空 false,不为空  true
     * @author 张念礼
     * @date 2012-4-13 上午10:53:30
     * @version V1.0.0
     * @ModifyRecord 修改记录
     * <br>1、张念礼 ; 2012-4-13 上午10:53:30; 初始化
     */
    public static final Boolean arrayIsNotEmpty(String[] args) {
        return !arrayIsEmpty(args);
    }

    /**
     * 概述：验证字符串是否为数字
     *
     * @param cs
     * @return boolean
     * @Title: isNumeric
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static final boolean isNumeric(final String cs) {
        if (isEmpty(cs)) {
            return false;
        }
        int n = 0;
        final int sz = cs.length();
        for (int i = 0; i < sz; i++) {
            char chars = cs.charAt(i);
            if (chars == '.') {
                n++;
            }
            if (n >= 2) {
                return false;
            }
            if (Character.isDigit(chars) == false && chars != '.') {
                return false;
            }
        }
        return true;
    }
    /**
     * 概述：判断字符串为有效的数字
     * @param cs
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static boolean isMathNumeric(final String cs){
    	if (isEmpty(cs)) {
            return false;
        }
        int n = 0;
        final int sz = cs.length();
        boolean firstIsZero = false;
        for (int i = 0; i < sz; i++) {
            char chars = cs.charAt(i);
            if (chars == '.') {
                n++;
            }
            if(i == 0 && '0' == chars){
            	firstIsZero = true; 
            }
            if(i == 1 && '.' != chars && firstIsZero){
            	return false;
            }
            if (n >= 2) {
                return false;
            }
            if (Character.isDigit(chars) == false && chars != '.') {
                return false;
            }
        }
        return true;
    }
    /**
     * 概述：验证Base64字符串
     *
     * @param base64      Base64字符串
     * @param chartLength Base64字符串长度(实际长度小于设置值,直接返回false)
     * @return true:是,false:否
     * boolean
     * @Title: isBase64
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static boolean isBase64(String base64, int chartLength) {
        if (base64 == null) {
            return false;
        }
        int length = base64.length();
        if (length < chartLength) {
            return false;
        }
        int count = 0;
        boolean numFlag = false;
        for (int i = 0; i < length; i++) {
            char charat = base64.charAt(i);
            if (charat == '=') {
                count++;
                continue;
            }
            if ((charat < '0' || charat > '9') && (charat < 'a' || charat > 'z') && (charat < 'A' || charat > 'Z')) {
                return false;
            }
            if ((!numFlag) && (charat >= '0' && charat <= '9')) {
                numFlag = true;
            }
        }
        if (count > 2 || (!numFlag)) {
            return false;
        }
        return true;
    }

    public static boolean isBase64(String base64) {
        if (base64 == null) {
            return false;
        }
        int length = base64.length();
        if (length % 4 != 0) {
            return false;
        }
        return isBase64(base64, 16);
    }

    /**
     * 概述：验证MD5字符串(16进制)
     *
     * @param md5         MD5字符串
     * @param chartLength MD5字符串长度(实际长度小于设置值,直接返回false),例:16,24,32等
     * @return true:是,false:否
     * boolean
     * @Title: isMD5
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static boolean isMD5(String md5, int chartLength) {
        if (md5 == null) {
            return false;
        }
        int digit = 0;
        int length = md5.length();
        if (length < chartLength) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            char charat = md5.charAt(i);
            digit = char2digit(charat);
            if (digit < 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean isMD5(String md5) {
        if (md5 == null) {
            return false;
        }
        int length = md5.length();
        if (length != 16 || length != 32) {
            return false;
        }
        return isMD5(md5, 0);
    }

    /**
     * 概述：验证UUID格式
     *
     * @param uuid UUID字符串
     * @return true:是,false:否
     * boolean
     * @Title: isUUID
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static boolean isUUID(String uuid) {
        if (uuid == null) {
            return false;
        }
        int radix = 16;
        int digit = 0;
        long result = 0;
        int splitCount = 0;
        int length = uuid.length();
        long limit = -Long.MAX_VALUE;
        long multmin = limit / radix;
        boolean isItem = false;
        for (int i = 0; i < length; i++) {
            char charat = uuid.charAt(i);
            if (charat == '-') {
                splitCount++;
                isItem = true;
                result = 0;
                continue;
            }
            digit = char2digit(charat);
            if (digit < 0) {
                return false;
            }
            if (i == 0 || isItem) {
                isItem = false;
                result = -digit;
                continue;
            }
            if (result < multmin) {
                return false;
            }
            result *= radix;
            if (result < limit + digit) {
                return false;
            }
            result -= digit;
        }
        if (splitCount != 4) {
            return false;
        }
        return true;
    }

    private static int char2digit(char charat) {
        int digit = -1;
        if (charat == 'a' || charat == 'A') {
            digit = 10;
        } else if (charat == 'b' || charat == 'B') {
            digit = 11;
        } else if (charat == 'c' || charat == 'C') {
            digit = 12;
        } else if (charat == 'd' || charat == 'D') {
            digit = 13;
        } else if (charat == 'e' || charat == 'E') {
            digit = 14;
        } else if (charat == 'f' || charat == 'F') {
            digit = 15;
        } else if (charat == '0') {
            digit = 0;
        } else if (charat == '1') {
            digit = 1;
        } else if (charat == '2') {
            digit = 2;
        } else if (charat == '3') {
            digit = 3;
        } else if (charat == '4') {
            digit = 4;
        } else if (charat == '5') {
            digit = 5;
        } else if (charat == '6') {
            digit = 6;
        } else if (charat == '7') {
            digit = 7;
        } else if (charat == '8') {
            digit = 8;
        } else if (charat == '9') {
            digit = 9;
        }
        return digit;
    }

    /**
     * 概述：连接字符串
     *
     * @param key 关键字符串集合
     * @return String
     * @Title: assembleKey
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String assembleKey(Object... key) {
        StringBuilder keyBuf = new StringBuilder();
        for (int i = 0; i < key.length; i++) {
            keyBuf.append(key[i]);
        }
        return keyBuf.toString();
    }

    /**
     * 概述：根据分割符连接字符串
     *
     * @param delim 分割符
     * @param key   关键字
     * @return String
     * @Title: assembleKey
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String assembleKeyByDelim(String delim, Object... key) {
        if (delim == null) {
            delim = "_";
        }
        StringBuilder keyBuf = new StringBuilder();
        for (int i = 0; i < key.length; i++) {
            keyBuf.append(key[i]);
            if (i < key.length - 1) {
                keyBuf.append(delim);
            }
        }
        return keyBuf.toString();
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

    public static final List<String> arr2set(String[] arr) {
        List<String> set = new ArrayList<String>();
        if (arr == null || arr.length == 0) {
            return set;
        }
        for (String str : arr) {
            if (set.indexOf(str) >= 0) {
                continue;
            }
            set.add(str);
        }
        Collections.sort(set);
        return set;
    }

    public static final List<String> arr2set(String str, String delim) {
        return arr2set(getSplit(str, delim));
    }
    /**
     * 概述：字符串转map结构
     * @param str
     * @param delim
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static final Map<Integer, String> getMap(String str, String delim){
    	Map<Integer, String> mapCache = new HashMap<Integer, String>();
    	List<String> listCache = arr2set(str, delim);
    	if(listCache == null || listCache.isEmpty()){
    		return mapCache;
    	}
    	for(int i = 0; i < listCache.size(); i++){
    		mapCache.put(i, listCache.get(i));
    	}
    	return mapCache;
    	
    }

    /**
     * 概述：是否为十六进制格式
     *
     * @param str 字段串
     * @return boolean
     * @Title: isHexNum
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static boolean isHexNum(String str) {
        if (isEmpty(str)) {
            return false;
        }
        int count = 0;
        if (str.startsWith("0x") || str.startsWith("0X")) {
            count = 2;
        } else {
            return false;
        }
        char[] chars = str.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (i < count) {
                continue;
            }
            char ch = chars[i];
            if ((ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') && (ch < 'A' || ch > 'F')) {
                return false;
            }
        }
        return true;
    }
    /**
     * 概述：获取第一个字母
     * @param str
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static String getFirsChar(String str){
		if(StringUtils.isEmpty(str)){
			return "";
		}
		char[] chars = str.toCharArray();
		String tmpChar = "";
		for(char word : chars){
			if((word >= 'A'&& word <= 'Z')||(word >= 'a' && word <= 'z')){
				tmpChar = String.valueOf(word);
				break;
			}
		}
		return tmpChar;
	}
}
