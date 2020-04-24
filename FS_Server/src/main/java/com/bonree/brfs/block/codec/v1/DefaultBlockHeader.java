package com.bonree.brfs.block.codec.v1;

import com.bonree.brfs.block.codec.ByteHolder;
import com.bonree.brfs.block.codec.ByteHolderBuilder;

public class DefaultBlockHeader implements ByteHolder {
	public static final byte HEAD_BYTE = (byte) 0xAC;
	
	private final byte version;
	private final byte validateType;
	
	public DefaultBlockHeader(int version, int validateType) {
		this.version = (byte) (version & 0xFF);
		this.validateType = (byte) (validateType & 0xFF);
	}
	
	public int length() {
		return 3;
	}
	
	public byte version() {
		return version;
	}
	
	public byte validateType() {
		return validateType;
	}

	@Override
	public byte[] toBytes() {
		return new byte[]{ HEAD_BYTE, version, validateType};
	}

	public static class Builder implements ByteHolderBuilder<DefaultBlockHeader> {
		private byte version;
		private byte type;
		
		public void setVersion(byte version) {
			this.version = version;
		}
		
		public void setValidateType(byte type) {
			this.type = type;
		}

		@Override
		public DefaultBlockHeader build() {
			return new DefaultBlockHeader(version, type);
		}
	}
}
