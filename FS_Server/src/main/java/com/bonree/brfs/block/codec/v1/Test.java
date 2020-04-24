package com.bonree.brfs.block.codec.v1;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class Test {

    public static void main(String[] args) throws IOException {
        ByteBuffer b = Files.map(new File("/root/temp/a"), MapMode.READ_ONLY);
        System.out.println(b.isReadOnly());

        b.position(3);
        b.limit(8);
        b = b.slice();
        System.out.println(b.get(0));
        System.out.println(b.get());
        System.out.println(b.get());
        System.out.println(b.get());
        System.out.println(b.get(0));

        System.out.println(b.get(b.limit() - 2));
        System.out.println(b.get(b.limit() - 1));

        MappedByteBuffer buffer = Files.map(new File("/root/temp/b"), MapMode.READ_WRITE, 1024 * 1024);
        buffer.putInt(1);
        buffer.putInt(2);
        buffer.putInt(3);

        System.out.println(buffer.isReadOnly());

        System.out.println(buffer.position() + ", " + buffer.limit());

        ByteBuffer bb = buffer.asReadOnlyBuffer();

        System.out.println(bb.isReadOnly());
        bb.flip();
        System.out.println(bb.position() + ", " + bb.limit());
        System.out.println(buffer.position() + ", " + buffer.limit());

        System.out.println(bb.getInt());
        System.out.println(bb.getInt());
        System.out.println(bb.getInt());
    }

}
