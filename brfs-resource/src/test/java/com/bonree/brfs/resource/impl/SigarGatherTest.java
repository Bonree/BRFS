package com.bonree.brfs.resource.impl;

import org.junit.Test;

public class SigarGatherTest {
    @Test
    public void testLibPath() throws Exception {
        System.out.println(new SigarGather().libPath());
    }

    @Test
    public void testconstruct() throws Exception {
        SigarGather gather = new SigarGather();
    }

    @Test
    public void testStart() throws Exception {
        SigarGather gather = new SigarGather();
        gather.start();
    }

    @Test(expected = IllegalStateException.class)
    public void testExpectException() throws Exception {
        SigarGather gather = new SigarGather();
        System.out.println(gather.collectOSInfo());
    }

    @Test
    public void testGathers() throws Exception {
        SigarGather gather = new SigarGather();
        System.out.println(gather.collectPartitionStats());
    }

    @Test
    public void testGatherSpecial() throws Exception {
        SigarGather gather = new SigarGather();
        gather.start();
        System.out.println(gather.collectSingleNetInfo("192.168.150.236"));
        System.out.println(gather.collectSingleNetStat("192.168.150.236"));
        System.out.println(gather.collectSinglePartitionInfo("/home/wellgeek/test/factory/brfs/data"));
        System.out.println(gather.collectSinglePartitionStats("/home/wellgeek/test/factory/brfs/data"));
    }
}
