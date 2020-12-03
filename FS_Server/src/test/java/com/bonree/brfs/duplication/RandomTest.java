package com.bonree.brfs.duplication;

import com.bonree.brfs.duplication.filenode.duplicates.impl.RandomPattern;
import com.bonree.brfs.duplication.filenode.duplicates.impl.Weightable;
import java.util.ArrayList;
import java.util.Random;
import org.junit.Test;

public class RandomTest {
    @Test
    public void getRandomIndex() {
        ArrayList<Weightable> weightMocks = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            weightMocks.add(new WeightMock(new Random().nextInt(100)));
        }
        for (int i = 0; i < 5; i++) {
            WeightMock weightMock = RandomPattern.randomWithWeight(weightMocks, WeightMock.class, 100);
            if (weightMock != null) {
                System.out.println(weightMock.weight);
            }
        }
    }

    @Test
    public void getRandom() {
        ArrayList<String> strings = new ArrayList<>();
        strings.add("a");
        strings.add("b");
        strings.add("c");
        for (int i = 0; i < 5; i++) {
            String o = RandomPattern.random(strings);
            if (o != null) {
                System.out.println(o);
            }
        }
    }

    class WeightMock implements Weightable {
        private long weight;

        public WeightMock(long weight) {
            this.weight = weight;
        }

        @Override
        public long weight() {
            return weight;
        }
    }
}
