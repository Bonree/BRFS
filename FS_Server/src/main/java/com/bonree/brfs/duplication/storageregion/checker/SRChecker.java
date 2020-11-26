package com.bonree.brfs.duplication.storageregion.checker;

/**
 * 在sr创建的时候检查sr name 格式
 */
public interface SRChecker {
    public boolean check(String srName);
}
