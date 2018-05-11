package com.bonree.brfs.authentication.test;

public class LocalDiskNode {
    private String dataDir;

    public LocalDiskNode(String dataDir) {
        this.dataDir = dataDir;
    }

    /** 概述：
     * @param remoteIP
     * @param remotePort
     * @param localPath
     * @param remotePath
     * @return 
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean copyTo(String remoteIP, String remotePort, String localPath, String remotePath) {
        return true;

    }

    /** 概述：
     * @param remoteIP
     * @param remotePort
     * @param localPath
     * @param remotePath
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean copyFrom(String remoteIP, String remotePort, String localPath, String remotePath) {
        return true;
    }

    /** 概述：
     * @param remoteIP
     * @param remotePort
     * @param remotePath
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean isExistFile(String remoteIP, String remotePort, String remotePath) {
        return false;

    }

    /** 概述：
     * @param remoteIP
     * @param remotePort
     * @param remotePath
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public long getFileSize(String remoteIP, String remotePort, String remotePath) {
        return 0;
    }

    /** 概述：
     * @param remoteIP
     * @param remotePort
     * @param remotePath
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String deleteFile(String remoteIP, String remotePort, String remotePath) {
        return remotePath;
    }

}
