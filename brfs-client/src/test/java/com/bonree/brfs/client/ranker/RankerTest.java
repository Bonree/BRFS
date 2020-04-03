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

import java.util.List;

import com.google.common.collect.ImmutableList;

public class RankerTest {

    public static void main(String[] args) {
        List<Integer> l = new ShiftRanker<Integer>().rank(ImmutableList.<Integer>of(1, 2, 3, 4, 5));
        
        System.out.println(l);
        System.out.println(l);
    }
    
}
