/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bonree.brfs.client.data.read;

import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;
import com.bonree.brfs.common.write.data.FileDecoder;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;

public class TcpFidContentReader implements FidContentReader {

    @Override
    public InputStream read(URI uri, String srName, Fid fidObj, long offset, long size, int uriIndex) throws Exception {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort()));

            socket.getOutputStream().write(toReadString(srName, fidObj, uriIndex));

            byte[] length = new byte[Integer.BYTES * 2];
            readBytes(socket.getInputStream(), length, 0, length.length);

            int l = Ints.fromBytes(length[4], length[5], length[6], length[7]);

            byte[] b = new byte[l];
            readBytes(socket.getInputStream(), b, 0, b.length);

            FileContent content = FileDecoder.contents(b);
            return new ByteArrayInputStream(content.getData().toByteArray());
        } finally {
            // do nothing
        }
    }

    private static byte[] toReadString(String srName, Fid fidObj, int index) {
        StringBuilder nameBuilder = new StringBuilder(fidObj.getUuid());
        String[] serverList = new String[fidObj.getServerIdCount()];
        for (int i = 0; i < fidObj.getServerIdCount(); i++) {
            String id = fidObj.getServerId(i);
            nameBuilder.append('_').append(id);
            serverList[i] = id;
        }

        return Joiner
            .on(';').useForNull("-").join(srName, index, fidObj.getTime(), fidObj.getDuration(),
                                          nameBuilder.toString(), null, fidObj.getOffset(), fidObj.getSize(), 0, 0, "\n")
            .getBytes(Charsets.UTF_8);
    }

    private static void readBytes(InputStream input, byte[] des, int offset, int length) throws IOException {
        int read = 0;

        while (length > 0 && (read = input.read(des, offset, length)) >= 0) {
            offset += read;
            length -= read;
        }
    }
}
