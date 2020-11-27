package com.bonree.brfs.duplication.storageregion.checker;

/**
 * T_<product>_xxx
 */
public class BRProductSRChecker implements SRChecker {
    @Override
    public boolean check(String srName) {
        if (!srName.startsWith("T")) {
            return false;
        }
        return srName.split("_").length > 2 ? true : false;
    }
}
