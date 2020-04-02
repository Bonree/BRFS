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
package com.bonree.brfs.client;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.bonree.brfs.client.data.DataSplitter;
import com.bonree.brfs.client.data.FixedSizeDataSplitter;

public class DataSplitterTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        DataSplitter splitter = new FixedSizeDataSplitter(3);
        
        Iterator<ByteBuffer> iter = splitter.split(new ByteArrayInputStream("1234567890".getBytes(StandardCharsets.UTF_8)));
        List<ByteBuffer> bs = new ArrayList<ByteBuffer>();
        while(iter.hasNext()) {
            ByteBuffer bb = iter.next();
            System.out.println("size:" + bb.toString() + (!iter.hasNext() ? "last" : "next"));
            
            bs.add(bb);
        }
        
        bs.forEach(buf -> {
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            
            System.out.println("-->" + new String(bytes));
        });
        
        Iterator<ByteBuffer> iter2 = splitter.split("1234567890".getBytes(StandardCharsets.UTF_8));
        List<ByteBuffer> bufs = new ArrayList<ByteBuffer>();
        while(iter2.hasNext()) {
            ByteBuffer bb = iter2.next();
            System.out.println("size2:" + bb.toString() + (!iter2.hasNext() ? "last" : "next"));
            bufs.add(bb);
        }
        
        bufs.forEach(buf -> {
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            
            System.out.println("-->" + new String(bytes));
        });
    }

}
