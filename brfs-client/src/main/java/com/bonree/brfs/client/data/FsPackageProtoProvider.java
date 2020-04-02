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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.bonree.brfs.client.ClientException;
import com.bonree.brfs.client.data.compress.Compression;
import com.bonree.brfs.client.utils.ByteBufferInputStream;
import com.bonree.brfs.client.utils.CrcUtils;
import com.bonree.brfs.common.proto.DataTransferProtos.FSPacketProto;
import com.google.protobuf.ByteString;

import okhttp3.MediaType;
import okhttp3.RequestBody;

public class FsPackageProtoProvider implements PutObjectRequestBodyProvider {
    private final MediaType contentType;
    
    public static final Context DEFAULT_CONTEXT = new Context() {
        
        @Override
        public boolean useCrc() {
            return false;
        }

        @Override
        public Compression getCompression() {
            return Compression.NONE;
        }
    };
    
    public FsPackageProtoProvider(MediaType contentType) {
        this.contentType = requireNonNull(contentType);
    }

    @Override
    public Iterator<RequestBody> from(
            Iterator<ByteBuffer> bufs,
            SequenceIDGenerator sequenceIDGenerator,
            int storageRegionId,
            Optional<String> fileName,
            Context context) {
        AtomicLong contentLengthAccumulator = new AtomicLong();
        Compression compression = context.getCompression() != null ? context.getCompression() : Compression.NONE;
        
        return new Iterator<RequestBody>() {

            @Override
            public boolean hasNext() {
                return bufs.hasNext();
            }

            @Override
            public RequestBody next() {
                return buildBody(
                        bufs.next(),
                        sequenceIDGenerator,
                        contentLengthAccumulator,
                        storageRegionId,
                        fileName,
                        context.useCrc(),
                        compression,
                        !bufs.hasNext());
            }
        };
    }

    private RequestBody buildBody(
            ByteBuffer buf,
            SequenceIDGenerator sequenceIDGenerator,
            AtomicLong contentLengthAccumulator,
            int storageRegionId,
            Optional<String> fileName,
            boolean useCrc,
            Compression compression,
            boolean noMoreData) {
        FSPacketProto.Builder builder = FSPacketProto.newBuilder();
        builder.setSeqno(sequenceIDGenerator.nextSequenceID());
        builder.setLastPacketInFile(noMoreData);
        builder.setStorageName(storageRegionId);
        if(fileName.isPresent()) {
            builder.setFileName(fileName.get());
        }
        
        builder.setCrcFlag(useCrc);
        if(useCrc) {
            builder.setCrcCheckCode(CrcUtils.crc(buf.duplicate()));
        }
        
        builder.setOffsetInFile(contentLengthAccumulator.addAndGet(buf.remaining()));
        builder.setCompress(compression.code());
        try {
            builder.setData(
                    ByteString.readFrom(
                            compression.compressor()
                            .compress(new ByteBufferInputStream(buf))));
        } catch (IOException e) {
            throw new ClientException(e, "Can not set data");
        }
        
        return RequestBody.create(builder.build().toByteArray(), contentType);
    }
}
