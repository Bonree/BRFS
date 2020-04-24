package com.bonree.brfs.block.codec;

import java.util.List;

import com.bonree.brfs.block.codec.v1.DefaultBlockHeader;
import com.bonree.brfs.block.codec.v1.DefaultBlockTailer;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;

public interface BlockDecoder {
	DefaultBlockHeader parseHeader() throws BlockDecodeException;
	DefaultBlockTailer parseTailer() throws BlockDecodeException;
	List<FileContent> parseAllData() throws BlockDecodeException;
	FileContent parseData(int offset, int size) throws BlockDecodeException;
}
