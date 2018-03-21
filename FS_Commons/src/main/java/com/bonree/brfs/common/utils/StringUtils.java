package com.bonree.brfs.common.utils;

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

}
