package com.bonree.brfs.common.utils;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

public class ListUtils {

    public static <T> Iterator<T> random(List<T> items) {
        BitSet visitedIndexes = new BitSet(items.size());
        return new Iterator<T>() {
            private int nextIndex = -1;

            private void nextIndex() {
                nextIndex = visitedIndexes.previousClearBit(ThreadLocalRandom.current().nextInt(items.size()));
                if (nextIndex == -1) {
                    nextIndex = visitedIndexes.previousClearBit(items.size() - 1);
                }
            }

            @Override
            public boolean hasNext() {
                if (nextIndex == -1) {
                    nextIndex();
                }

                return nextIndex != -1;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                try {
                    return items.get(nextIndex);
                } finally {
                    visitedIndexes.set(nextIndex);
                    nextIndex = -1;
                }
            }
        };
    }
}
