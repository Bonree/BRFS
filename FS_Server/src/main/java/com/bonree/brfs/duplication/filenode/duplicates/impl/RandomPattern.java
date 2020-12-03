package com.bonree.brfs.duplication.filenode.duplicates.impl;

import com.bonree.brfs.common.utils.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

public class RandomPattern {
    /**
     * 概述：权重随机数
     *
     * @param dents
     *
     * @return
     *
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static String getWeightRandom(final List<Pair<String, Integer>> dents, final Random random,
                                         Collection<String> unNeeds) {
        if (dents == null || dents.isEmpty() || random == null) {
            return null;
        }
        List<Pair<String, Integer>> source = new ArrayList<Pair<String, Integer>>();
        int total = 0;
        for (Pair<String, Integer> pair : dents) {
            if (unNeeds != null && unNeeds.contains(pair.getFirst())) {
                continue;
            }
            total += pair.getSecond();
            source.add(pair);
        }
        if (total == 0) {
            return null;
        }
        int randomNum = Math.abs(random.nextInt(total));
        int current = 0;
        for (Pair<String, Integer> ele : source) {
            current += ele.getSecond();
            if (randomNum > current) {
                continue;
            }
            if (randomNum <= current) {
                return ele.getFirst();
            }
        }
        return null;
    }

    /**
     * 根据权重在candidates中随机选择一个weightable
     * @param candidates 待选择的数组,其中的元素必须实现weigthtable接口以供计算权重
     * @param T 选中元素的实际类型
     * @param amplifier 放大倍数,辅助计算随机数的
     * @return 选中元素 未选中返回null
     */
    public static <T> T randomWithWeight(Collection<? extends Weightable> candidates, Class<T> clazz, int amplifier) {
        amplifier = amplifier < 100 ? 100 : amplifier;
        int random = new Random().nextInt(amplifier);
        long sum = candidates.stream()
                          .mapToLong(Weightable::weight)
                          .sum();
        int bound = 0;
        for (Weightable candidate :candidates) {
            bound += candidate.weight() * amplifier / sum;
            if (random < bound) {
                return (T) candidate;
            }
        }
        return null;
    }

    public static <T> T random(Collection<T> candidates) {
        int bound = new Random().nextInt(candidates.size());
        int count = 0;
        for (T candidate : candidates) {
            if (count++ >= bound) {
                return candidate;
            }
        }
        return null;
    }
}
