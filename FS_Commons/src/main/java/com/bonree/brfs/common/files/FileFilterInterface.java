package com.bonree.brfs.common.files;

import java.util.Map;

public interface FileFilterInterface {

    public String getKey(int index);

    public boolean isDeep(int index, Map<String, String> values);

    public boolean isAdd(String root, Map<String, String> values, boolean isFile);
}
