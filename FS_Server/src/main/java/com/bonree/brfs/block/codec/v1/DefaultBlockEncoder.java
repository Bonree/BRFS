package com.bonree.brfs.block.codec.v1;

import com.bonree.brfs.block.codec.BlockDataProcessException;
import com.bonree.brfs.block.codec.BlockEncoder;
import com.bonree.brfs.block.codec.ByteHolder;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;
import com.bonree.brfs.common.write.data.FileEncoder;

public class DefaultBlockEncoder implements BlockEncoder {
	
	public DefaultBlockEncoder() {
//		this.header = new DefaultBlockHeader(DEFAULT_HEADER_VERSION, DEFAULT_HEADER_VALIDATETYPE);
//		this.tailer = new DefaultBlockTailer();
	}

	@Override
	public DefaultBlockHeader.Builder newHeaderBuilder() {
		return new DefaultBlockHeader.Builder();
	}

	@Override
	public DefaultBlockTailer.Builder newTailerBuilder() {
		return new DefaultBlockTailer.Builder();
	}

	@Override
	public ByteHolder process(FileContent data) throws BlockDataProcessException {
		try {
			byte[] bytes = FileEncoder.contents(data);
			
			return () -> bytes;
		} catch (Exception e) {
			throw new BlockDataProcessException("process data error", e);
		}
	}

}
