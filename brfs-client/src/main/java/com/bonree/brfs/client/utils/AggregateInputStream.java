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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.google.common.io.Closeables;

public class AggregateInputStream extends InputStream {
    private final Iterator<InputStream> inputs;
    private InputStream currentStream;
    
    public AggregateInputStream(Iterator<InputStream> inputs) {
        this.inputs = inputs;
        this.currentStream = nextInput();
    }
    
    private InputStream nextInput() {
        return inputs.hasNext() ? inputs.next() : null;
    }

    @Override
    public int read() throws IOException {
        while(currentStream != null) {
            int b = currentStream.read();
            if(b != -1) {
                return b;
            }
            
            currentStream = nextInput();
        }
        
        return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        
        while(currentStream != null) {
            int readLength = currentStream.read(b, off, len);
            if(readLength != -1) {
                return readLength;
            }
            
            currentStream = nextInput();
        }
        
        return -1;
    }

    @Override
    public int available() throws IOException {
        return currentStream.available();
    }

    @Override
    public void close() throws IOException {
        Closeables.closeQuietly(currentStream);
        while(inputs.hasNext()) {
            Closeables.closeQuietly(inputs.next());
        }
    }

    
}
