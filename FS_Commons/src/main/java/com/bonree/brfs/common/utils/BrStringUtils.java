package com.bonree.brfs.common.utils;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;

public class BrStringUtils {

    private static final char SEPARATOR_DIR = '/';

    /** 概述：对basePath进行修剪
     * @param basePath 如：/aaa/bbb/ or /aaa/bbb
     * @return 修剪后的路径 如：/aaa/bbb
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public static String trimBasePath(String basePath) {
        String newBasePath = null;
        if (!org.apache.commons.lang3.StringUtils.isEmpty(basePath)) {
            byte ch = basePath.getBytes(StandardCharsets.UTF_8)[basePath.length() - 1];
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
            byte ch = basePath.getBytes(StandardCharsets.UTF_8)[basePath.length() - 1];
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
     * 从UTF-8格式的字节数组构造字符串
     * 
     * @param bytes
     * @return
     */
    public static String fromUtf8Bytes(byte[] bytes) {
        if (bytes != null) {
            try {
                return new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        return null;
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
        // 空对象
        if (null == args) {
            bool = true;
        }
        // 空字符串
        else if ("".equals(args.trim())) {
            bool = true;
        }
        // 全角
        else if ("　".equals(args)) {
            bool = true;
        }
        // null字符串
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

    /**
     * 概述：判断字符串为有效的数字
     * @param cs
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static boolean isMathNumeric(final String cs) {
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
            if (i == 0 && '0' == chars) {
                firstIsZero = true;
            }
            if (i == 1 && '.' != chars && firstIsZero) {
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
	 * 概述：验证字符串是否为数字
	 * 
	 * @Title: isNumeric
	 * @param cs
	 * @return boolean
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
			if(chars == '-' && i == 0){
				continue;
			}
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

    /** 概述：字符串转数字
     * @param numStr
     * @param cls
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public static <T extends Number> T parseNumber(String numStr, Class<T> cls) {
        T instance = null;
        try {
            instance = cls.getConstructor(String.class).newInstance(numStr);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        }
        if (instance == null) {
            throw new NumberFormatException("parse number numStr fail!!");
        }
        return instance;
    }
    

    /** 概述：解析字符为布尔类型
     * @param str
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public static boolean parseBoolean(String str) {
        return Boolean.parseBoolean(str);
    }

    /** 概述：检查字符不为空
     * @param str
     * @param desc
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public static void checkNotEmpty(String str, String desc) {
        if (StringUtils.isEmpty(str)) {
            throw new IllegalStateException(desc);
        }
    }

}
