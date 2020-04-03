package com.bonree.brfs.common.net.http;

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
	private boolean CONTINUE;
	private long nextSeqno;

	public HandleResult() {
		this(true);
	}
	
	public HandleResult(boolean success) {
		this.success = success;
	}

	public boolean isCONTINUE() {
		return CONTINUE;
	}

	public void setCONTINUE() {
		this.CONTINUE = true;
	}

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

	public void setNextSeqno(long seqno) {
		nextSeqno = seqno + 1;
	}

	public long getNextSeqno() {
		return nextSeqno;
	}
}
