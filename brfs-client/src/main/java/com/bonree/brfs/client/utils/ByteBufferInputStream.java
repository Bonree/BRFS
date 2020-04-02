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
package com.bonree.brfs.client.utils;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
    private final ByteBuffer buffer;
    
    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = requireNonNull(buffer);
    }

    @Override
    public int read() throws IOException {
        return buffer.hasRemaining() ? buffer.get() : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        
        if(!buffer.hasRemaining()) {
            return -1;
        }
        
        int readableLength = Math.min(len - off, buffer.remaining());
        buffer.get(b, off, readableLength);
        
        return readableLength;
    }

    @Override
    public long skip(long n) throws IOException {
        long skip = buffer.remaining();
        if(n < skip) {
            skip = n < 0 ? 0 : n;
        }
        
        int newPosition = buffer.position();
        buffer.position(Math.toIntExact(newPosition + skip));
        return skip;
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }
    
    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        buffer.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        buffer.reset();
    }

    @Override
    public void close() throws IOException {}

}
