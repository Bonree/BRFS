package com.bonree.brfs.schedulers.task.manager.impl;

import java.util.Map;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;

public class ReleaseTaskFactory {
	public static MetaTaskManagerInterface getInstance(int type){
		DefaultReleaseTask tmp = DefaultReleaseTask.getInstance();
		return tmp;
	}
}
