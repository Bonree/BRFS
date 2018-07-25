package com.bonree.brfs.common.filesync;


public final class SyncStateCodec {
	
	public static String toString(FileObjectSyncState state) {
		StringBuilder builder = new StringBuilder();
		builder.append(state.getServiceGroup()).append(":").append(state.getServiceId())
		       .append(state.getFilePath())
		       .append("?").append(state.getFileLength());
		
		return builder.toString();
	}
	
	public static FileObjectSyncState fromString(String s) {
		int serverSep = s.indexOf("/");
		if(serverSep < 0) {
			throw new IllegalArgumentException(s);
		}
		
		String[] serverInfo = s.substring(0, serverSep).trim().split(":");
		if(serverInfo.length != 2 || serverInfo[0].isEmpty()) {
			throw new IllegalArgumentException(s);
		}
		
		String[] fileInfo = s.substring(serverSep).trim().split("\\?");
		if(fileInfo.length != 2) {
			throw new IllegalArgumentException(s);
		}
		
		return new FileObjectSyncState(serverInfo[0], serverInfo[1], fileInfo[0], Long.parseLong(fileInfo[1]));
	}
}
