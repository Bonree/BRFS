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
package com.bonree.brfs.client.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.LongSupplier;

import com.bonree.brfs.client.ClientException;
import com.bonree.brfs.client.data.compress.Compression;
import com.bonree.brfs.client.utils.ByteBufferInputStream;
import com.bonree.brfs.client.utils.CrcUtils;
import com.bonree.brfs.client.utils.IteratorUtils.Transformer;
import com.bonree.brfs.common.proto.DataTransferProtos.FSPacketProto;
import com.google.protobuf.ByteString;

public class FSPackageProtoMaker implements Transformer<ByteBuffer, FSPacketProto> {
    private final LongSupplier sequenceGen;
    private final int storageRegionId;
    private final String fileId;
    private final Optional<String> fileName;
    private final boolean useCrc;
    private final Compression compression;
    
    private long contentLengthAccumulator;
    
    public FSPackageProtoMaker(
            LongSupplier sequenceGen,
            int storageRegionId,
            String fileId,
            Optional<String> fileName,
            boolean useCrc,
            Compression compression) {
        this.sequenceGen = sequenceGen;
        this.storageRegionId = storageRegionId;
        this.fileId = fileId;
        this.fileName = fileName;
        this.useCrc = useCrc;
        this.compression = compression;
    }

    @Override
    public FSPacketProto apply(ByteBuffer buffer, boolean noMoreElement) {
        FSPacketProto.Builder builder = FSPacketProto.newBuilder();
        builder.setSeqno(sequenceGen.getAsLong());
        builder.setLastPacketInFile(noMoreElement);
        builder.setStorageName(storageRegionId);
        builder.setWriteID(fileId);
        
        if(fileName.isPresent()) {
            builder.setFileName(fileName.get());
        }
        
        builder.setCrcFlag(useCrc);
        if(useCrc) {
            builder.setCrcCheckCode(CrcUtils.crc(buffer.duplicate()));
        }
        
        builder.setOffsetInFile(contentLengthAccumulator);
        contentLengthAccumulator += buffer.remaining();
        
        builder.setCompress(compression.code());
        try {
            builder.setData(
                    ByteString.readFrom(
                            compression.compressor()
                            .compress(new ByteBufferInputStream(buffer))));
        } catch (IOException e) {
            throw new ClientException(e, "Can not set data");
        }
        
        return builder.build();
    }

}
