package com.bonree.brfs.duplication.filenode;

import com.bonree.brfs.common.utils.TimeUtils;
import com.google.common.base.Splitter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePathBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(FilePathBuilder.class);

    private static final String PATH_SEPARATOR = "/";

    public static String buildFilePath(FileNode fileNode, String serviceId) {
        int index = 0;

        LOG.debug("build file path with sn[{}], serid[{}], time[{}] filename[{}]", fileNode.getStorageName(), serviceId,
                  fileNode.getCreateTime(), fileNode.getName());
        for (String id : Splitter.on("_").splitToList(fileNode.getName())) {
            if (id.equals(serviceId)) {
                break;
            }

            index++;
        }

        StringBuilder builder = new StringBuilder();
        builder.append(PATH_SEPARATOR)
            .append(fileNode.getStorageName())
            .append(PATH_SEPARATOR)
            .append(index)
            .append(PATH_SEPARATOR)
            .append(TimeUtils.timeInterval(fileNode.getCreateTime(), fileNode.getTimeDurationMillis()))
            .append(PATH_SEPARATOR)
            .append(fileNode.getName());

        return builder.toString();
    }

    public static String[] parsePath(String path) {
        List<String> parts = Splitter.on(PATH_SEPARATOR).splitToList(path);
        int index = Integer.parseInt(parts.get(parts.size() - 6));
        String secondId = Splitter.on("_").splitToList(parts.get(parts.size() - 1)).get(index);
        String storageRegionName = parts.get(parts.size() - 7);

        return new String[] {storageRegionName, secondId};
    }
}
