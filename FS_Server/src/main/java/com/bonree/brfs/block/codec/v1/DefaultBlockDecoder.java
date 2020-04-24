package com.bonree.brfs.block.codec.v1;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bonree.brfs.block.codec.BlockDecodeException;
import com.bonree.brfs.block.codec.BlockDecoder;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;
import com.bonree.brfs.common.write.data.FileDecoder;

public class DefaultBlockDecoder implements BlockDecoder {
	private final ByteBuffer buffer;
	
	private DefaultBlockHeader header;
	private DefaultBlockTailer tailer;
	private Map<Integer, FileContent> blockList = new HashMap<Integer, FileContent>();
	
	public DefaultBlockDecoder(ByteBuffer buffer) {
		this.buffer = buffer;
	}

	@Override
	public DefaultBlockHeader parseHeader() throws BlockDecodeException {
		if(header != null) {
			return header;
		}
		
		if((buffer.get(0) & 0xFF) != DefaultBlockHeader.HEAD_BYTE) {
			throw new BlockDecodeException("no valid header byte is found");
		}
		
		header = new DefaultBlockHeader(buffer.get(1), buffer.get(2));
		
		return header;
	}

	@Override
	public DefaultBlockTailer parseTailer() throws BlockDecodeException {
		if(tailer != null) {
			return tailer;
		}
		
		if((buffer.get(buffer.limit() - 1) & 0xFF) != DefaultBlockTailer.TAIL_BYTE) {
			throw new BlockDecodeException("no tailer header byte is found");
		}
		
		tailer = new DefaultBlockTailer(buffer.getLong(buffer.limit() - 1 - 8));
		
		return tailer;
	}

	@Override
	public List<FileContent> parseAllData() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FileContent parseData(int offset, int size) throws BlockDecodeException {
		FileContent content = blockList.get(offset);
		if(content != null) {
			return content;
		}
		
		ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
		readBuffer.position(offset + 3);
		readBuffer.limit(offset + 3 + size);
		byte[] bytes = readBuffer.slice().array();
		
		try {
			content = FileDecoder.contents(bytes);
			blockList.put(offset, content);
			
			return content;
		} catch (Exception e) {
			throw new BlockDecodeException("parse data error", e);
		}
	}

}
