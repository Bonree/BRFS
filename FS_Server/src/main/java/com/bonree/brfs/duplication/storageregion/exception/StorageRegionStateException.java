package com.bonree.brfs.duplication.storageregion.exception;

import com.bonree.brfs.client.utils.Strings;

public class StorageRegionStateException extends Exception {

    private static final long serialVersionUID = 4770090354473298041L;

    public StorageRegionStateException(String srName, Object expectedState, Object currentState) {
        super(Strings.format("storage region[%s] should be [%s], but [%s] now", srName, expectedState, currentState));
    }
}
