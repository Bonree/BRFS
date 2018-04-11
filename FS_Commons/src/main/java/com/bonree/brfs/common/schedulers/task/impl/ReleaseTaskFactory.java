package com.bonree.brfs.common.schedulers.task.impl;

import java.util.Map;

import com.bonree.brfs.common.schedulers.task.MetaTaskManagerInterface;
import com.bonree.brfs.common.utils.BrStringUtils;

public class ReleaseTaskFactory {
	public static MetaTaskManagerInterface getInstance(int type){
		DefaultReleaseTask tmp = DefaultReleaseTask.getInstance();
		return tmp;
	}
}
