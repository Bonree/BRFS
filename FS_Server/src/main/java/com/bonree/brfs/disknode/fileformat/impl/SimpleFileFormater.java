package com.bonree.brfs.disknode.fileformat.impl;

import javax.inject.Inject;

import com.bonree.brfs.common.proto.FileDataProtos.FileContent;
import com.bonree.brfs.common.write.data.FileEncoder;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.fileformat.FileHeader;
import com.bonree.brfs.disknode.fileformat.FileTailer;
import com.bonree.brfs.disknode.fileformat.FormatConfig;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class SimpleFileFormater implements FileFormater {
	private final long capacity;
	private FileHeader header = new SimpleFileHeader();
	private FileTailer tailer = new SimpleFileTailer();
	
	@Inject
	public SimpleFileFormater(FormatConfig config) {
	    this(config.getCapacity());
	}
	
	public SimpleFileFormater(long capacity) {
		Preconditions.checkArgument(capacity >= 0);
		this.capacity = capacity;
	}

	@Override
	public FileHeader fileHeader() {
		return header;
	}

	@Override
	public FileTailer fileTailer() {
		return tailer;
	}

	@Override
	public long maxBodyLength() {
		return capacity - fileHeader().length() - fileTailer().length();
	}

	@Override
	public long relativeOffset(long offset) {
		return offset - fileHeader().length();
	}

	@Override
	public long absoluteOffset(long offset) {
		return offset + fileHeader().length();
	}

	@Override
	public byte[] formatData(byte[] data) throws Exception {
		FileContent content = FileContent.newBuilder()
				.setCompress(Configs.getConfiguration().getConfig(DataNodeConfigs.CONFIG_DATA_COMPRESS) ? 1 : 0)
				.setDescription("")
				.setData(ByteString.copyFrom(data))
				.setCrcFlag(false)
				.setCrcCheckCode(0)
				.build();
		
		return FileEncoder.contents(content);
	}

}
