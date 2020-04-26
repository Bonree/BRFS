package com.bonree.brfs.disknode.utils;

/**
 * 二元组的实现类
 *
 * @param <V>
 * @param <T>
 *
 * @author yupeng
 */
public class Pair<V, T> {
    private final V first;
    private final T second;

    public Pair(V first, T second) {
        this.first = first;
        this.second = second;
    }

    public V first() {
        return this.first;
    }

    public T second() {
        return this.second;
    }
}
