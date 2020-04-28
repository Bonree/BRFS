package com.bonree.brfs.resource.impl;

import org.junit.Test;

public class SigarGatherTest {
    @Test
    public void testLibPath() {
        System.out.println(new SigarGather().libPath());
    }

    @Test
    public void testconstruct() {
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
        gather.start();
        System.out.println(gather.collectOSInfo());
        System.out.println(gather.collectCPUInfo());
        System.out.println(gather.collectCpuStat());
        System.out.println(gather.collectMemorySwapInfo());
        System.out.println(gather.collectMemStat());
        System.out.println(gather.collectSwapStat());
        System.out.println(gather.collectAverageLoad());
        System.out.println(gather.collectNetInfos());
        System.out.println(gather.collectNetStats());
        System.out.println(gather.collectPartitionInfos());
        System.out.println(gather.collectPartitionStats());
    }

    @Test
    public void testGatherSpecial() throws Exception {
        SigarGather gather = new SigarGather();
        gather.start();
        System.out.println(gather.collectSingleNetInfo("192.168.150.236"));
        System.out.println(gather.collectSingleNetStat("192.168.150.236"));
        System.out.println(gather.collectSinglePartitionInfo("/"));
        System.out.println(gather.collectSinglePartitionStats("/"));
    }
}
