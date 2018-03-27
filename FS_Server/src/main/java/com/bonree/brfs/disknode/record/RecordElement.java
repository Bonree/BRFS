package com.bonree.brfs.disknode.record;

/**
 * 数据文件写入记录信息
 * 
 * @author chen
 *
 */
public class RecordElement {
	
	//写入数据在文件中的偏移量
	private int offset;
	//写入数据的字节大小
	private int size;
	//写入数据的CRC码
	private long crc;
	
	public RecordElement() {
		this(0, 0);
	}
	
	public RecordElement(int offset, int size) {
		this(offset, size, 0L);
	}
	
	public RecordElement(int offset, int size, long crc) {
		this.offset = offset;
		this.size = size;
		this.crc = crc;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public long getCrc() {
		return crc;
	}

	public void setCrc(long crc) {
		this.crc = crc;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[").append(offset)
		       .append(", ").append(size)
		       .append(", ").append(crc)
		       .append("]");
		
		return builder.toString();
	}
}
