package com.bonree.brfs.tasks.maintain;

import org.junit.Test;

public class FileBlockMaintainerTest {
    @Test
    public void testDelayTime() {
        String zeroTime = "10:00";
        int delay = -1;
        delay = new FileBlockMaintainer().getDelayTime(zeroTime);
        System.out.println(delay);
    }
}
