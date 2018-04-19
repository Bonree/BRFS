package com.bonree.brfs.disknode;

import com.bonree.brfs.disknode.server.handler.WriteData;

public class InputEvent {
	private DiskWriter writer;
	private WriteData item;
	private InputEventCallback callback;
	
	public InputEvent(DiskWriter writer, WriteData item) {
		this.writer = writer;
		this.item = item;
	}
	
	public DiskWriter getWriter() {
		return writer;
	}

	public void setWriter(DiskWriter writer) {
		this.writer = writer;
	}
	
	public InputEventCallback getInputEventCallback() {
		return callback;
	}

	public void setInputEventCallback(InputEventCallback callback) {
		this.callback = callback;
	}

	public WriteData getItem() {
		return item;
	}

	public void setItem(WriteData item) {
		this.item = item;
	}
}
