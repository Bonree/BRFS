package com.bonree.brfs.disknode.utils;

import com.bonree.brfs.common.files.impl.BRFSFileBaseFilter;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.disknode.data.write.record.RecordFileBuilder;
import java.util.Map;

public class BRFSRdFileFilter extends BRFSFileBaseFilter {

    @Override
    public String getKey(int index) {
        return this.keyMap.get(index);
    }

    @Override
    public boolean isDeep(int index, Map<String, String> values) {
        return values.size() != this.keyMap.size();
    }

    @Override
    public boolean isAdd(String root, Map<String, String> values, boolean isFile) {
        if (values == null || values.isEmpty()) {
            return false;
        }
        if (!isFile) {
            return false;
        }
        if (values.size() != this.keyMap.size()) {
            return false;
        }
        String fileName = values.get(BRFSPath.FILE);

        return RecordFileBuilder.isRecordFile(fileName);

    }
}
