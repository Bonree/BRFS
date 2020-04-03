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

import java.util.AbstractList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ShiftRanker<E> implements Ranker<E> {

    @Override
    public List<E> rank(List<E> nodes) {
        if(nodes.isEmpty()) {
            return nodes;
        }
        
        return new ShiftedList<>(nodes, ThreadLocalRandom.current().nextInt(nodes.size()));
    }

    private static class ShiftedList<T> extends AbstractList<T> {
        private final List<T> delegate;
        private final int offset;
        
        public ShiftedList(List<T> delegate, int shiftOffset) {
            this.delegate = delegate;
            this.offset = shiftOffset;
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public T get(int index) {
            return delegate.get((index + offset) % delegate.size());
        }
        
    }
}
