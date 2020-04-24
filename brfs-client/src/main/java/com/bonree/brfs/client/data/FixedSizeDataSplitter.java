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

import com.bonree.brfs.client.utils.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FixedSizeDataSplitter implements DataSplitter {
    private final int maxSize;

    public FixedSizeDataSplitter(int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException(Strings.format("max size should > 0, but %d", maxSize));
        }

        this.maxSize = maxSize;
    }

    @Override
    public Iterator<ByteBuffer> split(InputStream input) {
        return new InputStreamIterator(input);
    }

    @Override
    public Iterator<ByteBuffer> split(byte[] bytes) {
        return new ByteArrayIterator(bytes);
    }

    private class InputStreamIterator implements Iterator<ByteBuffer> {
        private final InputStream input;
        private final byte[] bytes;
        private int readLength;

        public InputStreamIterator(InputStream input) {
            this.input = input;
            this.bytes = new byte[maxSize];

            advance();
        }

        private void advance() {
            try {
                readLength = input.read(bytes);
            } catch (IOException e) {
                readLength = -2;
            }
        }

        @Override
        public boolean hasNext() {
            return readLength > 0;
        }

        @Override
        public ByteBuffer next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                return ByteBuffer.wrap(bytes, 0, readLength);
            } finally {
                advance();
            }
        }

    }

    private class ByteArrayIterator implements Iterator<ByteBuffer> {
        private final ByteBuffer byteBuf;
        private int offset;

        public ByteArrayIterator(byte[] bytes) {
            this.byteBuf = ByteBuffer.wrap(bytes);
        }

        @Override
        public boolean hasNext() {
            return byteBuf.hasRemaining();
        }

        @Override
        public ByteBuffer next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                ByteBuffer next = byteBuf.duplicate();
                offset += Math.min(byteBuf.remaining(), maxSize);
                next.limit(offset);

                return next;
            } finally {
                byteBuf.position(offset);
            }
        }

    }
}
