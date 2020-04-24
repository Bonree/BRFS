package com.bonree.brfs.rebalance.task;

import java.util.Iterator;
import java.util.List;
import org.apache.curator.shaded.com.google.common.collect.Lists;

public class IteratorTest {

    public static void main(String[] args) {
        List<String> testList = Lists.newArrayList();
        testList.add("aaa");
        testList.add("bbb");
        testList.add("ccc");
        Iterator<String> it1 = testList.iterator();
        while (it1.hasNext()) {
            System.out.println(it1.next());
        }
        Iterator<String> it2 = testList.iterator();
        while (it2.hasNext()) {
            System.out.println(it2.next());
        }
    }

}
