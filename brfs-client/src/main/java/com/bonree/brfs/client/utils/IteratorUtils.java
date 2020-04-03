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

import java.util.Iterator;

public final class IteratorUtils {
    
    public static <T> IteratorHolder<T> from(Iterator<T> iter) {
        return new IteratorHolder<>(iter);
    }
    
    public static interface Transformer<T, R> {
        R apply(T t, boolean noMoreElement);
    }
    
    public static class IteratorHolder<T> {
        private final Iterator<T> iter;
        
        private IteratorHolder(Iterator<T> iter) {
            this.iter = iter;
        }
        
        public <R> IteratorHolder<R> map(Transformer<T, R> transformer) {
            return new IteratorHolder<R>(new TransformIterator<T, R>(iter, transformer));
        }
        
        public Iterator<T> iterator() {
            return iter;
        }
    }
    
    private static class TransformIterator<T, R> implements Iterator<R> {
        private final Iterator<T> iter;
        private final Transformer<T, R> transformer;
        
        public TransformIterator(Iterator<T> iter, Transformer<T, R> transformer) {
            this.iter = iter;
            this.transformer = transformer;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public R next() {
            return transformer.apply(iter.next(), !iter.hasNext());
        }
        
    }
    
    private IteratorUtils() {}
}
