package com.bonree.brfs.disknode.server.handler.data;

public class FileCopyMessage {
	public static final int DIRECT_FROM_REMOTE = 1;
	public static final int DIRECT_TO_REMOTE = 2;

	private int direct;
	private String remoteHost;
	private int remotePort;
	private String remotePath;
	private String localPath;

	public int getDirect() {
		return direct;
	}

	public void setDirect(int direct) {
		this.direct = direct;
	}

	public String getRemoteHost() {
		return remoteHost;
	}

	public void setRemoteHost(String remoteHost) {
		this.remoteHost = remoteHost;
	}

	public int getRemotePort() {
		return remotePort;
	}

	public void setRemotePort(int remotePort) {
		this.remotePort = remotePort;
	}

	public String getRemotePath() {
		return remotePath;
	}

	public void setRemotePath(String remotePath) {
		this.remotePath = remotePath;
	}

	public String getLocalPath() {
		return localPath;
	}

	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}
}
