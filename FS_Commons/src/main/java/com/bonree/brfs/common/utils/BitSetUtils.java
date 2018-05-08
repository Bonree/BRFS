package com.bonree.brfs.common.utils;

import java.util.BitSet;

public class BitSetUtils {
	
	/**
	 * 计算一系列位图的交集
	 * 
	 * @param sets
	 * @return
	 */
	public static BitSet intersect(BitSet[] sets) {
		BitSet set = new BitSet();
		
		if(sets == null || sets.length == 0) {
			return set;
		}
		
		set.or(sets[0]);
		for(int i = 1; i < sets.length; i++) {
			set.and(sets[i]);
		}
		
		return set;
	}
	
	/**
	 * 求一系列位图的并集
	 * 
	 * @param sets
	 * @return
	 */
	public static BitSet union(BitSet[] sets) {
		BitSet set = new BitSet();
		
		if(sets == null || sets.length == 0) {
			return set;
		}
		
		for(int i = 0; i < sets.length; i++) {
			set.or(sets[i]);
		}
		
		return set;
	}
	
	/**
	 * 求集合b相对于集合a的补集
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static BitSet minus(BitSet a, BitSet b) {
		BitSet set = new BitSet();
		
		if(a == null) {
			return set;
		}
		
		set.or(a);
		if(b == null) {
			return set;
		}
		
		set.andNot(b);
		return set;
	}
}
