package com.bonree.brfs.block.codec;

import java.nio.ByteBuffer;

public interface BlockCodec {
	BlockEncoder makeBuilder();
	BlockDecoder makeParser(String filePath);
	BlockDecoder makeParser(ByteBuffer buffer);
}
