package com.bonree.brfs.disknode.data.write.record;

/**
 * 数据文件写入记录信息
 * 
 * @author chen
 *
 */
public class RecordElement {
	private int sequence;
	//写入数据在文件中的偏移量
	private long offset;
	//写入数据的字节大小
	private int size;
	//写入数据的CRC码
	private long crc;

	public RecordElement() {
		this(0, 0);
	}
	
	public RecordElement(long offset, int size) {
		this(offset, size, 0L);
	}
	
	public RecordElement(long offset, int size, long crc) {
		this.offset = offset;
		this.size = size;
		this.crc = crc;
	}
	
	public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
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

	@Override
	public boolean equals(Object obj) {
		if(obj == null || !(obj instanceof RecordElement)) {
			return false;
		}
		
		RecordElement other = (RecordElement) obj;
		return offset == other.offset
				&& size == other.size
				&& crc == other.crc;
	}
}
