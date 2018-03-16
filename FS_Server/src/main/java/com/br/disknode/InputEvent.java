package com.br.disknode;

public class InputEvent {
	private DiskWriter writer;
	private byte[] data;
	private InputEventCallback callback;
	
	public InputEvent(DiskWriter writer, byte[] data) {
		this.writer = writer;
		this.data = data;
	}
	
	public DiskWriter getWriter() {
		return writer;
	}

	public void setWriter(DiskWriter writer) {
		this.writer = writer;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
	
	public InputEventCallback getInputEventCallback() {
		return callback;
	}

	public void setInputEventCallback(InputEventCallback callback) {
		this.callback = callback;
	}
}
