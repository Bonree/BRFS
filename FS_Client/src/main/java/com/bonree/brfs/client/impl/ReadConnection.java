package com.bonree.brfs.client.impl;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;

import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.apache.curator.shaded.com.google.common.base.Joiner;
import org.apache.curator.shaded.com.google.common.primitives.Ints;

import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;
import com.bonree.brfs.common.utils.InputUtils;
import com.bonree.brfs.common.write.data.FileDecoder;

public class ReadConnection implements Closeable {
	private Socket socket;
	
	public ReadConnection(Socket socket) {
		this.socket = socket;
	}
	
	public byte[] read(ReadObject readObject) throws Exception {
		socket.getOutputStream().write(Joiner.on(';')
				.useForNull("-")
				.join(readObject.getSn(),
						readObject.getIndex(),
						readObject.getTime(),
						readObject.getDuration(),
						readObject.getFileName(),
						readObject.getFilePath(),
						readObject.getOffset(),
						readObject.getLength(),
						readObject.getRaw(),
						readObject.getToken(),
						"\n").getBytes(Charsets.UTF_8));
		
		byte[] length = new byte[Integer.BYTES * 2];
		InputUtils.readBytes(socket.getInputStream(), length, 0, length.length);
		
		int l = Ints.fromBytes(length[4], length[5], length[6], length[7]);
		
		byte[] b = new byte[l];
		InputUtils.readBytes(socket.getInputStream(), b, 0, b.length);

		FileContent content = FileDecoder.contents(b);
        return content.getData().toByteArray();
	}

	@Override
	public void close() throws IOException {
		socket.close();
	}
}
