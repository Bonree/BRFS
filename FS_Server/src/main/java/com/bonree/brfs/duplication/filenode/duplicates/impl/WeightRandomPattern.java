package com.bonree.brfs.duplication.filenode.duplicates.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import com.bonree.brfs.common.utils.Pair;

public class WeightRandomPattern {
	/**
	 * 概述：权重随机数
	 * @param dents
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static String getWeightRandom(final List<Pair<String, Integer>> dents, final Random random, Collection<String> unNeeds){
		if(dents == null|| dents.isEmpty()|| random == null) {
			return null;
		}
		List<Pair<String,Integer>> source = new ArrayList<Pair<String,Integer>>();
		int total = 0;
		for(Pair<String,Integer> pair : dents) {
			if(unNeeds != null&& unNeeds.contains(pair.getFirst())) {
				continue;
			}
			total += pair.getSecond();
			source.add(pair);
		}
		if(total == 0) {
			return null;
		}
		int randomNum = Math.abs(random.nextInt()%total);
		int current = 0;
		for(Pair<String, Integer> ele : source){
			current += ele.getSecond();
			if(randomNum > current){
				continue;
			}
			if(randomNum <=current){
				return ele.getFirst();
			}
		}
		return null;
	}
}
