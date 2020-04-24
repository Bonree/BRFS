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

package com.bonree.brfs.metrics.data;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DataStatistic {
    private final long time;
    private final long count;

    @JsonCreator
    public DataStatistic(
        @JsonProperty("time") long time,
        @JsonProperty("count") long count) {
        this.time = time;
        this.count = count;
    }

    @JsonProperty("time")
    public long getTime() {
        return time;
    }

    @JsonProperty("count")
    public long getCount() {
        return count;
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
            .add("time", time)
            .add("count", count)
            .toString();
    }
}
