package com.bonree.brfs.common.collection;

import java.util.Comparator;
import java.util.Iterator;

public class SortedList<E> implements Iterable<E> {
    private Node<E> head = new Node<E>();
    private int size;

    private Comparator<E> comparator;

    public SortedList(Comparator<E> comparator) {
        this.comparator = comparator;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void clear() {
        head.next = null;
        size = 0;
    }

    private void insertAfter(Node<E> src, Node<E> target) {
        target.next = src.next;
        target.prev = src;
        src.next = target;

        if (target.next != null) {
            target.next.prev = target;
        }

        size++;
    }

    private void removeNode(Node<E> target) {
        target.prev.next = target.next;

        if (target.next != null) {
            target.next.prev = target.prev;
        }

        size--;
    }

    public void add(E item) {
        Node<E> node = new Node<E>();
        node.item = item;

        Node<E> insertNode = head;
        for (Node<E> x = head.next; x != null; x = x.next) {
            if (comparator.compare(x.item, item) < 0) {
                break;
            }

            insertNode = x;
        }

        insertAfter(insertNode, node);
    }

    public void remove(E item) {
        for (Node<E> x = head.next; x != null; x = x.next) {
            if (x.item.equals(item)) {
                removeNode(x);
                break;
            }
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new ItemIterator(head);
    }

    private static class Node<E> {
        public E item;
        public Node<E> prev;
        public Node<E> next;
    }

    private class ItemIterator implements Iterator<E> {
        private Node<E> pointer = new Node<E>();

        public ItemIterator(Node<E> first) {
            this.pointer = first;
        }

        @Override
        public boolean hasNext() {
            return pointer.next != null;
        }

        @Override
        public E next() {
            pointer = pointer.next;
            return pointer.item;
        }

        @Override
        public void remove() {
            removeNode(pointer);
        }

    }
}
