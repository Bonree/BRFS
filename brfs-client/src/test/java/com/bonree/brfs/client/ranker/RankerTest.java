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

package com.bonree.brfs.client.ranker;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RankerTest {

    public static void main(String[] args) {
        ShiftRanker<Integer> ranker = new ShiftRanker<>();
        List<Integer> nums = ImmutableList.<Integer>of(1, 2);
        Map<Integer, Integer> countMap = new HashMap<>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            int first = ranker.rank(nums).get(0);
            if (countMap.get(first) == null) {
                countMap.put(first, 1);
            } else {
                countMap.put(first, countMap.get(first) + 1);
            }
        }
        countMap.entrySet().stream().forEach(x -> {
            System.out.println(x.getKey() + "  = " + x.getValue());
        });
    }

}
