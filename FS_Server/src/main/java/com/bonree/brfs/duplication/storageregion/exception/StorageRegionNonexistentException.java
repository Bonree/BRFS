package com.bonree.brfs.duplication.storageregion.exception;

import com.bonree.brfs.client.utils.Strings;

public class StorageRegionNonexistentException extends Exception {
    private static final long serialVersionUID = 7325191699140082804L;

    public StorageRegionNonexistentException(String srName) {
        super(Strings.format("storage region[%s] is not existed", srName));
    }
}
