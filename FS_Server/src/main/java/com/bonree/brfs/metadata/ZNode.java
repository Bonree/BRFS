package com.bonree.brfs.metadata;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/16 15:00
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 描述ZK节点信息的单元
 ******************************************************************************/
public class ZNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<ZNode> children;
    private final Set<String> childrenNames;
    private ZNode parent;
    private String path;
    private byte[] data;
    private boolean isEphemeral;
    private long mtime;

    /**
     * Create new root node instance for a given path.
     */
    public ZNode(String path) {
        children = new LinkedList<>();
        childrenNames = new HashSet<>();
        parent = null;
        this.path = path;
        data = null;
        isEphemeral = false;
    }

    /**
     * Create new child node.
     */
    public ZNode(ZNode parent, String path) {
        children = new LinkedList<>();
        childrenNames = new HashSet<>();
        this.parent = parent;
        this.path = path;
        data = null;
    }

    public ZNode getParent() {
        return parent;
    }

    public void setParent(ZNode parent) {
        this.parent = parent;
    }

    public void appendChild(ZNode child) {
        children.add(child);
        childrenNames.add(child.getPath());
    }

    public List<ZNode> getChildren() {
        return children;
    }

    public Set<String> getChildrenNames() {
        return childrenNames;
    }

    /**
     * Get an absolute path of this node.
     */
    public String getAbsolutePath() {
        if (parent == null) { // root
            return path;
        } else {
            if ("/".equals(parent.getAbsolutePath())) { // parent is root
                return parent.getAbsolutePath() + path;
            } else {
                return parent.getAbsolutePath() + "/" + path;
            }
        }
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public boolean isEphemeral() {
        return isEphemeral;
    }

    public void setEphemeral(boolean isEphemeral) {
        this.isEphemeral = isEphemeral;
    }

    public long getMtime() {
        return mtime;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }
}
