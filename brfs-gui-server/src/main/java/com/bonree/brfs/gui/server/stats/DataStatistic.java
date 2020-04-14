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
package com.bonree.brfs.gui.server.stats;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DataStatistic {
    private final long writeCount;
    private final long readCount;
    
    @JsonCreator
    public DataStatistic(
            @JsonProperty("write") long writeCount,
            @JsonProperty("read") long readCount) {
        this.writeCount = writeCount;
        this.readCount = readCount;
    }

    @JsonProperty("write")
    public long getWriteCount() {
        return writeCount;
    }

    @JsonProperty("read")
    public long getReadCount() {
        return readCount;
    }
    
    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("writeCount", writeCount)
                .add("readCount", readCount)
                .toString();
    }
}
