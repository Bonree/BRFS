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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;

public class AggregateInputStreamTest {

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        ImmutableList<Supplier<InputStream>> list = ImmutableList.of(
                () -> new ByteArrayInputStream("1234567890".getBytes()),
                () -> new ByteArrayInputStream("first\nhahaha".getBytes()),
                () -> new ByteArrayInputStream("no\ncontinue".getBytes()),
                () -> new ByteArrayInputStream("here".getBytes()),
                () -> new ByteArrayInputStream("end".getBytes())
                );
        InputStream input = new LazeAggregateInputStream(list.iterator());
        
        BufferedReader r = new BufferedReader(new InputStreamReader(input));
        String l = null;
        while((l = r.readLine()) != null) {
            System.out.println(l);
        }
        
        r.close();
    }

}
