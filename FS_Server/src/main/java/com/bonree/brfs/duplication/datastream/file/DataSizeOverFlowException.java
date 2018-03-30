package com.bonree.brfs.duplication.datastream.file;

public class DataSizeOverFlowException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6161369650019517125L;

	public DataSizeOverFlowException(int inputSize, int maxSize) {
		super(buildMessage(inputSize, maxSize));
	}
	
	private static String buildMessage(int inputSize, int maxSize) {
		StringBuilder builder = new StringBuilder();
		builder.append("Max size is ")
		       .append(maxSize)
		       .append(", but get[")
		       .append(inputSize)
		       .append("]");
		
		return builder.toString();
	}
}
