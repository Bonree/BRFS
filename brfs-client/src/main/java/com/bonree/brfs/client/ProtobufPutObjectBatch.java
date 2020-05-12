package com.bonree.brfs.client;

import com.bonree.brfs.client.data.compress.Compression;
import com.bonree.brfs.client.utils.CrcUtils;
import com.bonree.brfs.common.proto.DataTransferProtos.FSPacketProto;
import com.bonree.brfs.common.proto.DataTransferProtos.WriteBatch;
import com.google.protobuf.ByteString;
import java.util.Optional;

public class ProtobufPutObjectBatch implements PutObjectBatch {

    private final WriteBatch batch;

    private ProtobufPutObjectBatch(WriteBatch batch) {
        this.batch = batch;
    }

    @Override
    public int size() {
        return batch.getItemsCount();
    }

    @Override
    public byte[] toByteArray() {
        return batch.toByteArray();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private final WriteBatch.Builder builder;

        private Builder() {
            this.builder = WriteBatch.newBuilder();
        }

        public Builder putObject(byte[] item) {
            builder.addItems(buildData(Optional.empty(), item, false, Compression.NONE));
            return this;
        }

        public Builder putObject(String path, byte[] item) {
            builder.addItems(buildData(Optional.of(path), item, false, Compression.NONE));
            return this;
        }

        private FSPacketProto buildData(Optional<String> path, byte[] bytes, boolean useCrc, Compression compression) {
            FSPacketProto.Builder builder = FSPacketProto.newBuilder();
            builder.setSeqno(0);
            builder.setLastPacketInFile(true);

            if (path.isPresent()) {
                builder.setFileName(path.get());
            }

            builder.setCrcFlag(useCrc);
            if (useCrc) {
                builder.setCrcCheckCode(CrcUtils.crc(bytes));
            }

            builder.setOffsetInFile(0);
            builder.setCompress(compression.code());
            builder.setData(ByteString.copyFrom(compression.compressor().compress(bytes)));

            return builder.build();
        }

        public ProtobufPutObjectBatch build() {
            return new ProtobufPutObjectBatch(builder.build());
        }
    }
}
