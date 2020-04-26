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

package com.bonree.brfs.gui.server;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TimedData<T> {
    private final long time;
    private final T data;

    public TimedData(
        @JsonProperty("time") long time,
        @JsonProperty("data") T data) {
        this.time = time;
        this.data = data;
    }

    @JsonProperty("time")
    public long getTime() {
        return time;
    }

    @JsonProperty("data")
    public T getData() {
        return data;
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
            .add("time", time)
            .add("data", data)
            .toString();
    }
}
