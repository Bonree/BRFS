package com.bonree.brfs.duplication.datastream;

import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FilePathBuilder;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;

public class IDFilePathMaker implements FilePathMaker {

    @Override
    public String buildPath(FileNode fileNode, DuplicateNode dupNode) {
        return FilePathBuilder.buildFilePath(fileNode, dupNode.getSecondId());
    }

}
