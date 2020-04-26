package com.bonree.brfs.duplication.datastream;

import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;

public interface FilePathMaker {
    String buildPath(FileNode fileNode, DuplicateNode dupNode);
}
