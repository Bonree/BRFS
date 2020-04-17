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

import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Charsets;
import com.google.common.io.ByteSource;

public interface BRFSObject {
    InputStream getObjectContent();
    
    default String string() {
        try(InputStream input = getObjectContent()) {
            return new ByteSource() {

                @Override
                public InputStream openStream() throws IOException {
                    return input;
                }
            }
            .asCharSource(Charsets.UTF_8)
            .read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    default byte[] byteArray() {
        try(InputStream input = getObjectContent()) {
            return new ByteSource() {

                @Override
                public InputStream openStream() throws IOException {
                    return input;
                }
            }
            .read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    static BRFSObject from(InputStream input) {
        return new BRFSObject() {
            
            @Override
            public InputStream getObjectContent() {
                return input;
            }
        };
    }
}
