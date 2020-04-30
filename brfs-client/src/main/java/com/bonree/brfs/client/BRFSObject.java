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

import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public interface BRFSObject {

    /**
     * the stream of data from file.
     *
     * @return the input stream should be closed after consumed.
     */
    InputStream getObjectContent();

    /**
     * consume the bytes of stream and transfer them to String.
     *
     * @return String of stream
     */
    default String string() {
        byte[] bytes = byteArray();
        if (bytes == null) {
            return null;
        }

        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * consume the bytes of stream.
     *
     * @return bytes of stream
     */
    default byte[] byteArray() {
        try (InputStream input = getObjectContent()) {
            if (input == null) {
                return null;
            }

            return ByteStreams.toByteArray(input);
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
