package com.bonree.brfs.duplication.synchronize;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.coordinator.FileNode;

public class SynchronierErrorRecorder {
	private static final Logger LOG = LoggerFactory.getLogger(SynchronierErrorRecorder.class);
	private static final String FIELD_SEPARATOR = " ";
	
	private final File errorFile;
	
	public SynchronierErrorRecorder(File file) {
		this.errorFile = file;
	}
	
	public void writeErrorFile(FileNode fileNode) {
		FileOutputStream output = null;
		try {
			output = new FileOutputStream(errorFile, true);
			
			StringBuilder builder = new StringBuilder();
			builder.append(TimeUtils.formatTimeStamp(fileNode.getCreateTime())).append(FIELD_SEPARATOR)
			       .append(fileNode.getStorageName()).append(FIELD_SEPARATOR)
			       .append(fileNode.getName()).append("\n");
			
			output.write(BrStringUtils.toUtf8Bytes(builder.toString()));
		} catch (IOException e) {
			LOG.error("writeErrorFile[{}] error", fileNode.getName(), e);
		} finally {
			CloseUtils.closeQuietly(output);
		}
	}
}
