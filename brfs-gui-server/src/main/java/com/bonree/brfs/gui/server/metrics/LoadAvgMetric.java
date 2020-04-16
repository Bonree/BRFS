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
package com.bonree.brfs.gui.server.metrics;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LoadAvgMetric {
    private final long time;
    private final double load;
    
    @JsonCreator
    public LoadAvgMetric(
            @JsonProperty("time") long time,
            @JsonProperty("load") double load) {
        this.time = time;
        this.load = load;
    }

    @JsonProperty("time")
    public long getTime() {
        return time;
    }

    @JsonProperty("load")
    public double getLoad() {
        return load;
    }
    
    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("time", time)
                .add("load", load)
                .toString();
    }
}
