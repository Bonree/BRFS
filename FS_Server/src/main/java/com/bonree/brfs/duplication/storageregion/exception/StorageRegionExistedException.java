package com.bonree.brfs.duplication.storageregion.exception;

import com.bonree.brfs.client.utils.Strings;

public class StorageRegionExistedException extends Exception {

    private static final long serialVersionUID = 4815370576618682366L;

    public StorageRegionExistedException(String srName) {
        super(Strings.format("storage region[%s] is already existed.", srName));
    }

}
