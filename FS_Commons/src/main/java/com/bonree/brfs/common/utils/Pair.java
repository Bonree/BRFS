package com.bonree.brfs.common.utils;

public class Pair<T1, T2> {
    private T1 first;
    private T2 second;

    public Pair(T1 key, T2 value) {
        this.first = key;
        this.second = value;
    }

    public Pair() {

    }

    public String toString() {
        return this.first + ":" + this.second;
    }

    public boolean isBothEmpty() {
        return isEmptyFirst() && isEmptySecond();
    }

    public boolean isEmptyFirst() {
        return this.first == null;
    }

    public boolean isEmptySecond() {
        return this.second == null;
    }

    public T1 getFirst() {
        return first;
    }

    public void setFirst(T1 first) {
        this.first = first;
    }

    public T2 getSecond() {
        return second;
    }

    public void setSecond(T2 second) {
        this.second = second;
    }

}
