package com.bonree.brfs.common.http;

/**
 * {@link MessageHandler}的处理结果对象
 * 
 * @author chen
 *
 */
public class HandleResult {
	private boolean success;
	private Throwable cause;
	private byte[] data;

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public Throwable getCause() {
		return cause;
	}

	public void setCause(Throwable cause) {
		this.cause = cause;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
}
