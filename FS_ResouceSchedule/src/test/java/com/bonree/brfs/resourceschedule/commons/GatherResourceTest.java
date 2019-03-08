package com.bonree.brfs.resourceschedule.commons;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class GatherResourceTest{
    @Test
    @SuppressWarnings("all")
    public void testcalcRemain(){
        Map<String,Long> map = new HashMap<>();
        Map<String,Long> dmap = new HashMap<>();
        String key = "a";
        map.put(key,1L);
        dmap.put(key,2L);
        System.out.println(GatherResource.calcRemainRate(map,dmap));
    }
    @Test
    @SuppressWarnings("all")
    public void testcalcRemainError01(){
        Map<String,Long> map = new HashMap<>();
        Map<String,Long> dmap = new HashMap<>();
        String key = "a";
        map.put(key,1L);
        dmap.put(key,0L);
        System.out.println(GatherResource.calcRemainRate(map,dmap));
    }
    @Test
    @SuppressWarnings("all")
    public void testcalcRemainError02(){
        Map<String,Long> map = new HashMap<>();
        Map<String,Long> dmap = new HashMap<>();
        String key = "a";
        map.put(key,1L);
        System.out.println(GatherResource.calcRemainRate(map,dmap));
    }
}
