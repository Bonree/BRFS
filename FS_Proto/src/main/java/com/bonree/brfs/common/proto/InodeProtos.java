// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Inode.proto

package com.bonree.brfs.common.proto;

public final class InodeProtos {
  private InodeProtos() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface InodeValueProtoOrBuilder extends
      // @@protoc_insertion_point(interface_extends:InodeValueProto)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <pre>
     * 在rockDB中的唯一id,只有目录有这个id
     * </pre>
     *
     * <code>optional int64 inodeID = 1;</code>
     */
    boolean hasInodeID();
    /**
     * <pre>
     * 在rockDB中的唯一id,只有目录有这个id
     * </pre>
     *
     * <code>optional int64 inodeID = 1;</code>
     */
    long getInodeID();

    /**
     * <pre>
     * fid，只有文件有这个fid
     * </pre>
     *
     * <code>optional string fid = 2;</code>
     */
    boolean hasFid();
    /**
     * <pre>
     * fid，只有文件有这个fid
     * </pre>
     *
     * <code>optional string fid = 2;</code>
     */
    java.lang.String getFid();
    /**
     * <pre>
     * fid，只有文件有这个fid
     * </pre>
     *
     * <code>optional string fid = 2;</code>
     */
    com.google.protobuf.ByteString
        getFidBytes();
  }
  /**
   * Protobuf type {@code InodeValueProto}
   */
  public  static final class InodeValueProto extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:InodeValueProto)
      InodeValueProtoOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use InodeValueProto.newBuilder() to construct.
    private InodeValueProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private InodeValueProto() {
      inodeID_ = 0L;
      fid_ = "";
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private InodeValueProto(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              inodeID_ = input.readInt64();
              break;
            }
            case 18: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000002;
              fid_ = bs;
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.bonree.brfs.common.proto.InodeProtos.internal_static_InodeValueProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.bonree.brfs.common.proto.InodeProtos.internal_static_InodeValueProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.bonree.brfs.common.proto.InodeProtos.InodeValueProto.class, com.bonree.brfs.common.proto.InodeProtos.InodeValueProto.Builder.class);
    }

    private int bitField0_;
    public static final int INODEID_FIELD_NUMBER = 1;
    private long inodeID_;
    /**
     * <pre>
     * 在rockDB中的唯一id,只有目录有这个id
     * </pre>
     *
     * <code>optional int64 inodeID = 1;</code>
     */
    public boolean hasInodeID() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <pre>
     * 在rockDB中的唯一id,只有目录有这个id
     * </pre>
     *
     * <code>optional int64 inodeID = 1;</code>
     */
    public long getInodeID() {
      return inodeID_;
    }

    public static final int FID_FIELD_NUMBER = 2;
    private volatile java.lang.Object fid_;
    /**
     * <pre>
     * fid，只有文件有这个fid
     * </pre>
     *
     * <code>optional string fid = 2;</code>
     */
    public boolean hasFid() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <pre>
     * fid，只有文件有这个fid
     * </pre>
     *
     * <code>optional string fid = 2;</code>
     */
    public java.lang.String getFid() {
      java.lang.Object ref = fid_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          fid_ = s;
        }
        return s;
      }
    }
    /**
     * <pre>
     * fid，只有文件有这个fid
     * </pre>
     *
     * <code>optional string fid = 2;</code>
     */
    public com.google.protobuf.ByteString
        getFidBytes() {
      java.lang.Object ref = fid_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        fid_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeInt64(1, inodeID_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, fid_);
      }
      unknownFields.writeTo(output);
    }

    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(1, inodeID_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, fid_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.bonree.brfs.common.proto.InodeProtos.InodeValueProto)) {
        return super.equals(obj);
      }
      com.bonree.brfs.common.proto.InodeProtos.InodeValueProto other = (com.bonree.brfs.common.proto.InodeProtos.InodeValueProto) obj;

      boolean result = true;
      result = result && (hasInodeID() == other.hasInodeID());
      if (hasInodeID()) {
        result = result && (getInodeID()
            == other.getInodeID());
      }
      result = result && (hasFid() == other.hasFid());
      if (hasFid()) {
        result = result && getFid()
            .equals(other.getFid());
      }
      result = result && unknownFields.equals(other.unknownFields);
      return result;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasInodeID()) {
        hash = (37 * hash) + INODEID_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getInodeID());
      }
      if (hasFid()) {
        hash = (37 * hash) + FID_FIELD_NUMBER;
        hash = (53 * hash) + getFid().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(com.bonree.brfs.common.proto.InodeProtos.InodeValueProto prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code InodeValueProto}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:InodeValueProto)
        com.bonree.brfs.common.proto.InodeProtos.InodeValueProtoOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.bonree.brfs.common.proto.InodeProtos.internal_static_InodeValueProto_descriptor;
      }

      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.bonree.brfs.common.proto.InodeProtos.internal_static_InodeValueProto_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.bonree.brfs.common.proto.InodeProtos.InodeValueProto.class, com.bonree.brfs.common.proto.InodeProtos.InodeValueProto.Builder.class);
      }

      // Construct using com.bonree.brfs.common.proto.InodeProtos.InodeValueProto.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      public Builder clear() {
        super.clear();
        inodeID_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        fid_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.bonree.brfs.common.proto.InodeProtos.internal_static_InodeValueProto_descriptor;
      }

      public com.bonree.brfs.common.proto.InodeProtos.InodeValueProto getDefaultInstanceForType() {
        return com.bonree.brfs.common.proto.InodeProtos.InodeValueProto.getDefaultInstance();
      }

      public com.bonree.brfs.common.proto.InodeProtos.InodeValueProto build() {
        com.bonree.brfs.common.proto.InodeProtos.InodeValueProto result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.bonree.brfs.common.proto.InodeProtos.InodeValueProto buildPartial() {
        com.bonree.brfs.common.proto.InodeProtos.InodeValueProto result = new com.bonree.brfs.common.proto.InodeProtos.InodeValueProto(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.inodeID_ = inodeID_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.fid_ = fid_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder clone() {
        return (Builder) super.clone();
      }
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return (Builder) super.setField(field, value);
      }
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return (Builder) super.clearField(field);
      }
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return (Builder) super.clearOneof(oneof);
      }
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return (Builder) super.setRepeatedField(field, index, value);
      }
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return (Builder) super.addRepeatedField(field, value);
      }
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.bonree.brfs.common.proto.InodeProtos.InodeValueProto) {
          return mergeFrom((com.bonree.brfs.common.proto.InodeProtos.InodeValueProto)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.bonree.brfs.common.proto.InodeProtos.InodeValueProto other) {
        if (other == com.bonree.brfs.common.proto.InodeProtos.InodeValueProto.getDefaultInstance()) return this;
        if (other.hasInodeID()) {
          setInodeID(other.getInodeID());
        }
        if (other.hasFid()) {
          bitField0_ |= 0x00000002;
          fid_ = other.fid_;
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.bonree.brfs.common.proto.InodeProtos.InodeValueProto parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.bonree.brfs.common.proto.InodeProtos.InodeValueProto) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private long inodeID_ ;
      /**
       * <pre>
       * 在rockDB中的唯一id,只有目录有这个id
       * </pre>
       *
       * <code>optional int64 inodeID = 1;</code>
       */
      public boolean hasInodeID() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <pre>
       * 在rockDB中的唯一id,只有目录有这个id
       * </pre>
       *
       * <code>optional int64 inodeID = 1;</code>
       */
      public long getInodeID() {
        return inodeID_;
      }
      /**
       * <pre>
       * 在rockDB中的唯一id,只有目录有这个id
       * </pre>
       *
       * <code>optional int64 inodeID = 1;</code>
       */
      public Builder setInodeID(long value) {
        bitField0_ |= 0x00000001;
        inodeID_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * 在rockDB中的唯一id,只有目录有这个id
       * </pre>
       *
       * <code>optional int64 inodeID = 1;</code>
       */
      public Builder clearInodeID() {
        bitField0_ = (bitField0_ & ~0x00000001);
        inodeID_ = 0L;
        onChanged();
        return this;
      }

      private java.lang.Object fid_ = "";
      /**
       * <pre>
       * fid，只有文件有这个fid
       * </pre>
       *
       * <code>optional string fid = 2;</code>
       */
      public boolean hasFid() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <pre>
       * fid，只有文件有这个fid
       * </pre>
       *
       * <code>optional string fid = 2;</code>
       */
      public java.lang.String getFid() {
        java.lang.Object ref = fid_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            fid_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <pre>
       * fid，只有文件有这个fid
       * </pre>
       *
       * <code>optional string fid = 2;</code>
       */
      public com.google.protobuf.ByteString
          getFidBytes() {
        java.lang.Object ref = fid_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          fid_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <pre>
       * fid，只有文件有这个fid
       * </pre>
       *
       * <code>optional string fid = 2;</code>
       */
      public Builder setFid(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        fid_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * fid，只有文件有这个fid
       * </pre>
       *
       * <code>optional string fid = 2;</code>
       */
      public Builder clearFid() {
        bitField0_ = (bitField0_ & ~0x00000002);
        fid_ = getDefaultInstance().getFid();
        onChanged();
        return this;
      }
      /**
       * <pre>
       * fid，只有文件有这个fid
       * </pre>
       *
       * <code>optional string fid = 2;</code>
       */
      public Builder setFidBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        fid_ = value;
        onChanged();
        return this;
      }
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:InodeValueProto)
    }

    // @@protoc_insertion_point(class_scope:InodeValueProto)
    private static final com.bonree.brfs.common.proto.InodeProtos.InodeValueProto DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new com.bonree.brfs.common.proto.InodeProtos.InodeValueProto();
    }

    public static com.bonree.brfs.common.proto.InodeProtos.InodeValueProto getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<InodeValueProto>
        PARSER = new com.google.protobuf.AbstractParser<InodeValueProto>() {
      public InodeValueProto parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
          return new InodeValueProto(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<InodeValueProto> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<InodeValueProto> getParserForType() {
      return PARSER;
    }

    public com.bonree.brfs.common.proto.InodeProtos.InodeValueProto getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_InodeValueProto_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_InodeValueProto_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\013Inode.proto\"/\n\017InodeValueProto\022\017\n\007inod" +
      "eID\030\001 \001(\003\022\013\n\003fid\030\002 \001(\tB+\n\034com.bonree.brf" +
      "s.common.protoB\013InodeProtos"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_InodeValueProto_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_InodeValueProto_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_InodeValueProto_descriptor,
        new java.lang.String[] { "InodeID", "Fid", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
